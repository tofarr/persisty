import dataclasses
from typing import Optional, List

from servey.security.authorization import Authorization, AuthorizationError

from persisty.store_meta import get_meta
from persisty.secured.secured_store_factory_abc import SecuredStoreFactoryABC
from persisty.batch_edit import BatchEdit
from persisty.batch_edit_result import BatchEditResult
from persisty.store.store_abc import StoreABC
from persisty.store_meta import StoreMeta
from persisty.store.wrapper_store_abc import WrapperStoreABC
from servey_main.models.message import Message
from servey_main.storage import message_store_factory

STORAGE_META = get_meta(Message)
STORAGE_META = dataclasses.replace(
    STORAGE_META,
    # We make sure that the author_id is not updatable, and will not appear as such in the external APIs
    attrs=tuple(
        dataclasses.replace(f, updatable=False, creatable=False) if f.name == 'author_id' else f
        for f in STORAGE_META.attrs
    ),
)


@dataclasses.dataclass
class SecuredMessageStore(WrapperStoreABC[Message]):
    store: StoreABC[Message]
    authorization: Authorization

    def get_store(self) -> StoreABC:
        return self.store

    def create(self, item: Message) -> Message:
        item.author_id = self.authorization.subject_id  # Auto set the author
        return self.store.create(item)

    def _delete(self, key: str, item: Message) -> bool:
        if item.author_id != self.authorization.subject_id:
            raise AuthorizationError('forbidden')  # Can't edit messages created by others
        return self.store._delete(key, item)

    def delete(self, key: str) -> bool:
        if not self.authorization.has_scope('admin') or key == self.authorization.subject_id:
            raise AuthorizationError('forbidden')
        return self.store.delete(key)

    def edit_batch(self, edits: List[BatchEdit[Message]]) -> List[BatchEditResult[Message]]:
        for edit in edits:
            if edit.create_item:
                edit.create_item.author_id = self.authorization.subject_id
        return self.store.edit_batch(edits)


class SecuredMessageStoreFactory(SecuredStoreFactoryABC[Message]):

    def get_meta(self) -> StoreMeta:
        return STORAGE_META

    def create(self, authorization: Optional[Authorization]) -> Optional[StoreABC[Message]]:
        return SecuredMessageStore(message_store_factory.create(), authorization)


secured_message_store_factory = SecuredMessageStoreFactory()
