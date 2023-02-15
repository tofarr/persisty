from dataclasses import dataclass
from typing import Optional

from servey.security.authorization import Authorization

from persisty.store.store_abc import StoreABC
from persisty.store.wrapper_store_abc import WrapperStoreABC, T


@dataclass
class OwnedStore(WrapperStoreABC[T]):
    """
    Store implementation which sets an attribute value to the subject_id of an owner on create or update.
    This class does not actually perform filtering - it should wrap a FilteredStore to accomplish that
    """
    store: StoreABC[T]
    authorization: Authorization
    attr_name: str

    def get_store(self) -> StoreABC:
        return self.store

    def create(self, item: T) -> T:
        setattr(item, self.attr_name, self.authorization.subject_id)
        return self.get_store().create(item)

    def _update(self, key: str, item: T, updates: T) -> Optional[T]:
        if (
            getattr(item, self.attr_name) != self.authorization.subject_id
            or getattr(updates, self.attr_name) != self.authorization.subject_id
        ):
            setattr(item, self.attr_name, self.authorization.subject_id)
        return self.get_store()._update(key, item, updates)