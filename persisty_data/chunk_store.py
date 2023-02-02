from dataclasses import dataclass
from typing import Optional

from persisty.store.store_abc import StoreABC
from persisty.store.wrapper_store_abc import WrapperStoreABC
from persisty.util import UNDEFINED
from persisty_data.chunk import Chunk
from persisty_data.upload import Upload, UploadStatus


@dataclass
class ChunkStore(WrapperStoreABC[Chunk]):
    """
    Store which validates a few things about chunks being uploaded. For example, it validates that chunk sizes are
    not greater than the max size, and that chunks are all associated with an active upload.
    """
    chunk_store: StoreABC[Chunk]
    upload_store: StoreABC[Upload]
    max_chunk_size: int = 1024 * 1024 * 5

    def get_store(self) -> StoreABC[Chunk]:
        return self.chunk_store

    def create(self, item: Chunk) -> Chunk:
        assert len(item.data) <= self.max_chunk_size
        assert item.part_number >= 1
        upload = self.upload_store.read(item.upload_id)
        assert upload.status == UploadStatus.IN_PROGRESS
        return self.chunk_store.create(item)

    def _update(self, key: str, item: Chunk, updates: Chunk) -> Optional[Chunk]:
        if updates.part_number is not UNDEFINED:
            assert item.part_number == updates.part_number
        assert len(item.data) <= self.max_chunk_size
        return self.chunk_store._update(key, item, updates)

    def _delete(self, key: str, item: Chunk) -> bool:
        upload = self.upload_store.read(item.upload_id)
        assert upload.status == UploadStatus.IN_PROGRESS
        return self.get_store()._delete(key, item)
