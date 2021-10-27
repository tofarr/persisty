from dataclasses import dataclass
from datetime import datetime
from typing import Iterator, TypeVar, Callable

from persisty.edit import Edit
from persisty.edit_type import EditType
from persisty.store.store_abc import StoreABC
from persisty.store.wrapper_store_abc import WrapperStoreABC

T = TypeVar('T')


def timestamp_str():
    return datetime.now().isoformat()


@dataclass(frozen=True)
class TimestampStore(WrapperStoreABC[T]):
    """ Store which updates timestamps on items prior to storage. """
    store: StoreABC[T]
    created_at_attr: str = 'created_at'
    updated_at_attr: str = 'updated_at'
    timestamp: Callable = timestamp_str

    @property
    def name(self) -> str:
        return self.store.name

    def create(self, item: T) -> str:
        now = self.timestamp()
        setattr(item, self.created_at_attr, now)
        setattr(item, self.updated_at_attr, now)
        return self.store.create(item)

    def update(self, item: T) -> T:
        setattr(item, self.updated_at_attr, self.timestamp())
        return self.store.update(item)

    def edit_all(self, edits: Iterator[Edit[T]]):
        edits = (self._process_edit(e) for e in edits)
        return self.store.edit_all(edits)

    def _process_edit(self, edit):
        if edit.edit_type == EditType.CREATE:
            now = self.timestamp()
            setattr(edit.value, self.created_at_attr, now)
            setattr(edit.value, self.updated_at_attr, now)
        elif edit.edit_type == EditType.UPDATE:
            now = self.timestamp()
            setattr(edit.value, self.updated_at_attr, now)
        return edit
