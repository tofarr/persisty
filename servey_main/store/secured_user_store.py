import dataclasses
from typing import Optional, List

from servey.security.authorization import Authorization, AuthorizationError

from persisty.secured.secured_store_factory_abc import SecuredStoreFactoryABC
from persisty.batch_edit import BatchEdit
from persisty.batch_edit_result import BatchEditResult
from persisty.store.store_abc import StoreABC
from persisty.store.wrapper_store_abc import WrapperStoreABC
from persisty.store_access import StoreAccess
from persisty.store_meta import StoreMeta, get_meta
from persisty.trigger.wrapper import triggered_store
from servey_main.models.user import User
from servey_main.store import user_store_factory

STORAGE_META = get_meta(User)
STORAGE_META = dataclasses.replace(
    STORAGE_META,
    # The secured store meta does not include the password digest, as we don't want this available through web
    attrs=tuple(f for f in STORAGE_META.attrs if f.name != "password_digest"),
    # All create operations are handled from a sign up action where a password is supplied
    store_access=StoreAccess(creatable=False),
)


@dataclasses.dataclass
class SecuredUserStore(WrapperStoreABC[User]):
    store: StoreABC
    authorization: Authorization

    def get_store(self) -> StoreABC:
        return self.store

    def _update(
        self,
        key: str,
        item: User,
        updates: User,
    ) -> Optional[User]:
        """
        Users are allowed to edit themselves. Root users can edit anybody, but cannot remove the root flag
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


class SecuredUserStoreFactory(SecuredStoreFactoryABC[User]):
    def get_meta(self) -> StoreMeta:
        return STORAGE_META

    def create(
        self, authorization: Optional[Authorization]
    ) -> Optional[StoreABC[User]]:
        store = SecuredUserStore(user_store_factory.create(), authorization)
        store = triggered_store(store)
        return store


secured_user_store_factory = SecuredUserStoreFactory()
