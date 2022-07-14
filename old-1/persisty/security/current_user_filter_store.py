from dataclasses import dataclass
from typing import Iterator

from persisty.edit import Edit
from persisty.edit_type import EditType
from persisty.errors import PersistyError
from persisty.item_filter import AttrFilter, AttrFilterOp
from persisty.security.current_user import get_current_user
from persisty.storage.storage_abc import StorageABC, T
from persisty.storage.wrappers.filtered_storage import FilteredStorage
from persisty.storage.wrappers.wrapper_storage_abc import WrapperStorageABC


@dataclass(frozen=True)
class CurrentUserFilterStorage(WrapperStorageABC[T]):
    """
    Storage which filters objects to those 'owned' by the current user
    """
    wrapped_storage: StorageABC[T]
    user_key_attr: str = 'id'
    filter_attr: str = 'user_id'

    @property
    def _current_user_key(self):
        user = get_current_user()
        if user is None:
            raise PersistyError('missing_current_user')
        user_key = getattr(user, self.user_key_attr)
        return user_key

    @property
    def storage(self) -> StorageABC[T]:
        item_filter = AttrFilter(self.filter_attr, AttrFilterOp.eq, self._current_user_key)
        return FilteredStorage(self.wrapped_storage, item_filter)

    def create(self, item: T) -> str:
        setattr(item, self.filter_attr, self._current_user_key)
        return super().create(item)

    def update(self, item: T) -> T:
        setattr(item, self.filter_attr, self._current_user_key)
        return super().update(item)

    def edit_all(self, edits: Iterator[Edit[T]]):
        edits = iter(edits)
        edits = self._filter_edits(edits)
        return self.storage.edit_all(edits)

    def _filter_edits(self, edits: Iterator[Edit[T]]) -> Iterator[Edit[T]]:
        for edit in edits:
            if edit.edit_type in [EditType.CREATE, EditType.UPDATE]:
                setattr(edit.item, self.filter_attr, self._current_user_key)
            yield edit
