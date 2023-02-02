import base64
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.batch_edit import BatchEdit
from persisty.search_order.search_order import SearchOrder
from persisty.search_order.search_order_attr import SearchOrderAttr
from persisty.store.store_abc import StoreABC
from persisty.store.wrapper_store_abc import WrapperStoreABC
from persisty_data.chunk import Chunk
from persisty_data.content_meta import ContentMeta
from persisty_data.upload import Upload, UploadStatus


@dataclass
class UploadStore(WrapperStoreABC[Upload]):
    """
    Store which validates a few things about uploads. It insures that only IN_PROGRESS uploads can be updated,
    and that
    """
    upload_store: StoreABC[Upload]
    chunk_store: StoreABC[Chunk]
    content_meta_store: StoreABC[ContentMeta]
    chunk_size: int = 1024 * 1024 * 5
    upload_expire_in: int = 3600

    def get_store(self) -> StoreABC[Chunk]:
        return self.chunk_store

    def create(self, upload: Upload) -> Upload:
        assert upload.status == UploadStatus.IN_PROGRESS
        upload.expire_in = datetime.fromtimestamp(datetime.now().timestamp() + self.upload_expire_in, tz=timezone.utc)
        return self.upload_store.create(upload)

    def _update(self, key: str, item: Upload, updates: Upload) -> Optional[Upload]:
        assert item.status == UploadStatus.IN_PROGRESS
        result = self.upload_store._update(key, item, updates)
        if not result:
            return
        if item.status == updates.status:
            return result
        if updates.status == UploadStatus.COMPLETED:
            content_meta = self.content_meta_store.read(key)
            etag, size_in_bytes = self._process_chunks(result.id)
            if content_meta:
                content_meta.upload_id = result.id
                content_meta.content_type = result.content_type
                content_meta.etag = etag
                content_meta.size_in_bytes = size_in_bytes
                self.content_meta_store.update(content_meta)
            else:
                self.content_meta_store.create(ContentMeta(
                    key=key,
                    upload_id=result.id,
                    content_type=result.content_type,
                    etag=etag,
                    size_in_bytes=size_in_bytes
                ))
        elif updates.status in (UploadStatus.ABORTED, UploadStatus.TIMED_OUT):
            self._delete_all_chunks(item.id)
        return result

    def _delete(self, key: str, item: Chunk) -> bool:
        upload = self.upload_store.read(item.upload_id)
        assert upload.status == UploadStatus.IN_PROGRESS
        return self.get_store()._delete(key, item)

    def _delete_all_chunks(self, upload_id: str):
        chunk_key_config = self.chunk_store.get_meta().key_config
        chunks = self.chunk_store.search_all(AttrFilter('upload_id', AttrFilterOp.eq, upload_id))
        edits = (BatchEdit(delete_key=chunk_key_config.to_key_str(c)) for c in chunks)
        self.chunk_store.edit_all(edits)

    def _process_chunks(self, upload_id: str):
        upload = self.upload_store.read(upload_id)
        search_filter = AttrFilter('upload_id', AttrFilterOp.eq, upload.id)
        chunks = self.chunk_store.search_all(
            search_filter=search_filter,
            search_order=SearchOrder((SearchOrderAttr('part_number'),))
        )
        prev_chunk = None
        sha1 = hashlib.sha1()
        size_in_bytes = 0
        for chunk in chunks:
            if prev_chunk:
                # Chunks except last one are validated against min size
                assert len(prev_chunk.data) != self.chunk_size
            sha1.update(chunk.data)
            size_in_bytes = len(chunk.data)
            prev_chunk = chunk

        etag = base64.b64encode(sha1.digest()).decode('UTF-8')
        return etag, size_in_bytes
