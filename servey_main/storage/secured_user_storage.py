import dataclasses
from typing import Optional, List

from marshy.types import ExternalItemType
from servey.security.authorization import Authorization, AuthorizationError

from persisty.obj_storage.stored import get_storage_meta
from persisty.secured.secured_storage_factory_abc import SecuredStorageFactoryABC
from persisty.storage.batch_edit import BatchEdit
from persisty.storage.batch_edit_result import BatchEditResult
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_access import StorageAccess
from persisty.storage.storage_meta import StorageMeta
from persisty.storage.wrapper_storage_abc import WrapperStorageABC
from servey_main.models.user import User
from servey_main.storage import user_storage_factory

STORAGE_META = get_storage_meta(User)
STORAGE_META = dataclasses.replace(
    STORAGE_META,
    # The secured storage meta does not include the password digest, as we don't want this available through web
    fields=tuple(f for f in STORAGE_META.fields if f.name != 'password_digest'),
    # All create operations are handled from a sign up action where a password is supplied
    storage_access=StorageAccess(creatable=False)
)


@dataclasses.dataclass
class SecuredUserStorage(WrapperStorageABC):
    storage: StorageABC
    authorization: Authorization

    def get_storage(self) -> StorageABC:
        return self.storage

    def _update(
        self,
        key: str,
        item: ExternalItemType,
        updates: ExternalItemType,
    ) -> Optional[ExternalItemType]:
        """
        Users are allowed to edit themselves. Root users can edit anybody, but cannot remove the root flag
        from themselves.
        """
        if self.authorization.has_scope('admin'):
            if item['id'] == self.authorization.subject_id and item['admin'] is False:
                raise AuthorizationError('forbidden')  # Can't remove admin permission from self
        elif item['id'] != self.authorization.subject_id or item['admin'] is True:
            raise AuthorizationError('forbidden')  # Can't edit others or add admin permission to self
        return self.storage._update(key, item, updates)

    def delete(self, key: str) -> bool:
        if not self.authorization.has_scope('admin') or key == self.authorization.subject_id:
            raise AuthorizationError('forbidden')
        return self.storage.delete(key)

    def edit_batch(self, edits: List[BatchEdit]) -> List[BatchEditResult]:
        if not self.authorization.has_scope('admin'):
            raise AuthorizationError('forbidden')  # only admins can use the batch operation
        for edit in edits:
            if edit.create_item:
                raise AuthorizationError('forbidden')  # Can't create users directly
            if (
                edit.update_item
                and edit.update_item['id'] == self.authorization.subject_id
                and edit.update_item['admin'] is False
            ):
                raise AuthorizationError('forbidden')  # Can't remove admin from self
            if edit.delete_key == self.authorization.subject_id:
                raise AuthorizationError('forbidden')  # Can't delete self
        for edit in edits:
            if edit.create_item or (edit.delete_key == self.authorization.subject_id):
                raise AuthorizationError('forbidden')
        return self.storage.edit_batch(edits)


class SecuredUserStorageFactory(SecuredStorageFactoryABC):

    def get_storage_meta(self) -> StorageMeta:
        return STORAGE_META

    def create(self, authorization: Optional[Authorization]) -> Optional[StorageABC]:
        return SecuredUserStorage(user_storage_factory.create(), authorization)


secured_user_storage_factory = SecuredUserStorageFactory()
