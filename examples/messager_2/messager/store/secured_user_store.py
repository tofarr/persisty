import dataclasses
from dataclasses import dataclass
from typing import Optional, List

from persisty.batch_edit import BatchEdit
from persisty.batch_edit_result import BatchEditResult
from persisty.store.store_abc import StoreABC
from persisty.store.wrapper_store_abc import WrapperStoreABC
from persisty.store_meta import StoreMeta
from servey.security.authorization import Authorization, AuthorizationError

from messager.store.user import User


@dataclass
class SecuredUserStore(WrapperStoreABC[User]):
    """
    Customized store wrapper which implements ownership logic, and removes the
    password digest field from any external listing
    """

    store: StoreABC
    authorization: Authorization

    def get_meta(self) -> StoreMeta:
        # We dont want the password digest to be included / updated in the User items
        # included in the web interface.
        meta = self.store.get_meta()
        attrs = tuple(f for f in meta.attrs if f.name != "password_digest")
        return dataclasses.replace(meta, attrs=attrs)

    def get_store(self) -> StoreABC:
        return self.store

    def _update(
        self,
        key: str,
        item: User,
        updates: User,
    ) -> Optional[User]:
        """
        Users are allowed to edit themselves. Admin users can edit anybody, but cannot remove the admin flag
        from themselves.
        """
        if self.authorization.has_scope("admin"):
            if item.id == self.authorization.subject_id and item.admin is False:
                raise AuthorizationError(
                    "forbidden"
                )  # Can't remove admin permission from self
        elif item.id != self.authorization.subject_id or item.admin is True:
            raise AuthorizationError(
                "forbidden"
            )  # Can't edit others or add admin permission to self
        return self.store._update(key, item, updates)

    def delete(self, key: str) -> bool:
        if (
            not self.authorization.has_scope("admin")
            or key == self.authorization.subject_id
        ):
            raise AuthorizationError("forbidden")
        return self.store.delete(key)

    def edit_batch(
        self, edits: List[BatchEdit[User, User]]
    ) -> List[BatchEditResult[User, User]]:
        if not self.authorization.has_scope("admin"):
            raise AuthorizationError(
                "forbidden"
            )  # only admins can use the batch operation
        for edit in edits:
            if edit.create_item:
                raise AuthorizationError("forbidden")  # Can't create users directly
            if (
                edit.update_item
                and edit.update_item.id == self.authorization.subject_id
                and edit.update_item.admin is False
            ):
                raise AuthorizationError("forbidden")  # Can't remove admin from self
            if edit.delete_key == self.authorization.subject_id:
                raise AuthorizationError("forbidden")  # Can't delete self
        for edit in edits:
            if edit.create_item or (edit.delete_key == self.authorization.subject_id):
                raise AuthorizationError("forbidden")
        return self.store.edit_batch(edits)
