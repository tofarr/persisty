"""
This module wraps stores for users to add security constraints.
* The password_digest attribute should not be readable or updatable by the standard mechanisms
* A user should be able to self edit, but not edit other users unless they have the admin flag
* A special sign up process is required to create users
"""

import dataclasses
from typing import Optional, List

from servey.security.authorization import Authorization, AuthorizationError

from persisty.batch_edit import BatchEdit
from persisty.batch_edit_result import BatchEditResult
from persisty.factory.store_factory_abc import StoreFactoryABC
from persisty.store.store_abc import StoreABC
from persisty.store.wrapper_store_abc import WrapperStoreABC
from persisty.store_access import StoreAccess
from persisty.store_meta import StoreMeta, get_meta
from persisty.trigger.wrapper import triggered_store
from messager.models.user import User
from messager.store import user_store

_STORE_META = get_meta(User)
_STORE_META = dataclasses.replace(
    _STORE_META,
    # We dont want the password digest to be included / updated in the User items included in the web interface.
    attrs=tuple(f for f in _STORE_META.attrs if f.name != "password_digest"),
    # All create operations are handled from a sign up action where a password is supplied
    store_access=StoreAccess(creatable=False),
)


@dataclasses.dataclass
class SecuredUserStore(WrapperStoreABC[User]):
    store: StoreABC
    authorization: Authorization

    def get_meta(self) -> StoreMeta:
        return _STORE_META

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


class SecuredUserStoreFactory(StoreFactoryABC[User]):
    def get_meta(self) -> StoreMeta:
        return _STORE_META

    def create(
        self, authorization: Optional[Authorization]
    ) -> Optional[StoreABC[User]]:
        store = SecuredUserStore(user_store, authorization)
        store = triggered_store(store)
        return store


user_store_factory = SecuredUserStoreFactory()
