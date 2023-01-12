import dataclasses
from typing import Optional, List

from marshy.types import ExternalItemType
from servey.security.authorization import Authorization, AuthorizationError

from persisty.obj_storage.stored import get_storage_meta
from persisty.secured.secured_storage_factory_abc import SecuredStorageFactoryABC
from persisty.storage.batch_edit import BatchEdit
from persisty.storage.batch_edit_result import BatchEditResult
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta
from persisty.storage.wrapper_storage_abc import WrapperStorageABC
from servey_main.models.message import Message
from servey_main.storage import message_storage_factory

STORAGE_META = get_storage_meta(Message)
STORAGE_META = dataclasses.replace(
    STORAGE_META,
    # We make sure that the author_id is not updatable, and will not appear as such in the external APIs
    attrs=tuple(
        dataclasses.replace(f, is_updatable=False, is_creatable=False) if f.name == 'author_id' else f
        for f in STORAGE_META.attrs
    ),
)


@dataclasses.dataclass
class SecuredMessageStorage(WrapperStorageABC):
    storage: StorageABC
    authorization: Authorization

    def get_storage(self) -> StorageABC:
        return self.storage

    def create(self, item: ExternalItemType) -> ExternalItemType:
        item['author_id'] = self.authorization.subject_id  # Auto set the author
        return self.storage.create(item)

    def _delete(self, key: str, item: ExternalItemType) -> bool:
        if item['author_id'] != self.authorization.subject_id:
            raise AuthorizationError('forbidden')  # Can't edit messages created by others
        return self.storage._delete(key, item)

    def delete(self, key: str) -> bool:
        if not self.authorization.has_scope('admin') or key == self.authorization.subject_id:
            raise AuthorizationError('forbidden')
        return self.storage.delete(key)

    def edit_batch(self, edits: List[BatchEdit]) -> List[BatchEditResult]:
        for edit in edits:
            if edit.create_item:
                edit.create_item['author_id'] = self.authorization.subject_id
        return self.storage.edit_batch(edits)


class SecuredMessageStorageFactory(SecuredStorageFactoryABC):

    def get_storage_meta(self) -> StorageMeta:
        return STORAGE_META

    def create(self, authorization: Optional[Authorization]) -> Optional[StorageABC]:
        return SecuredMessageStorage(message_storage_factory.create(), authorization)


secured_message_storage_factory = SecuredMessageStorageFactory()
