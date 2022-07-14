from dataclasses import dataclass
from datetime import datetime
from typing import Iterator, Callable, Any, Iterable

from persisty.attr.attr import Attr
from persisty.attr.attr_access_control import AttrAccessControl
from persisty.attr.attr_mode import AttrMode
from persisty.cache_control.timestamp_cache_control import TimestampCacheControl
from persisty.edit import Edit
from persisty.edit_type import EditType
from persisty.storage.storage_abc import StorageABC, T
from persisty.storage.storage_meta import StorageMeta
from persisty.storage.wrappers.wrapper_storage_abc import WrapperStorageABC

ACCESS_CONTROL = AttrAccessControl(AttrMode.EXCLUDED, AttrMode.EXCLUDED, AttrMode.REQUIRED, AttrMode.REQUIRED)


@dataclass(frozen=True)
class TimestampedStorage(WrapperStorageABC[T]):
    """ Storage which updates timestamps on items prior to storage. """
    wrapped_storage: StorageABC[T]
    created_at_attr: str = 'created_at'
    updated_at_attr: str = 'updated_at'
    timestamp: Callable[[], Any] = datetime.now

    @property
    def storage(self):
        return self.wrapped_storage

    @property
    def meta(self) -> StorageMeta:
        meta = self.wrapped_storage.meta
        return StorageMeta(
            name=meta.name,
            attrs=tuple(self._filter_attrs(meta.attrs)),
            key_config=meta.key_config,
            access_control=meta.access_control,
            cache_control=TimestampCacheControl(meta.cache_control, self.updated_at_attr)
        )

    def _filter_attrs(self, attrs: Iterable[Attr]) -> Iterable[Attr]:
        """ created_at / updated_at attrs are included in the response but not in create / update """
        for attr in attrs:
            if attr.name in [self.created_at_attr, self.updated_at_attr]:
                yield Attr(attr.name, attr.schema, ACCESS_CONTROL)
            else:
                yield attr

    def create(self, item: T) -> str:
        now = self.timestamp()
        setattr(item, self.created_at_attr, now)
        setattr(item, self.updated_at_attr, now)
        return self.storage.create(item)

    def update(self, item: T) -> T:
        setattr(item, self.updated_at_attr, self.timestamp())
        return self.storage.update(item)

    def edit_all(self, edits: Iterator[Edit[T]]):
        edits = (self._process_edit(e) for e in edits)
        return self.storage.edit_all(edits)

    def _process_edit(self, edit):
        if edit.edit_type == EditType.CREATE:
            now = self.timestamp()
            setattr(edit.item, self.created_at_attr, now)
            setattr(edit.item, self.updated_at_attr, now)
        elif edit.edit_type == EditType.UPDATE:
            now = self.timestamp()
            setattr(edit.item, self.updated_at_attr, now)
        return edit


def with_timestamps(storage: StorageABC):
    meta = storage.meta
    timestamp_attrs = [a for a in meta.attrs if a.name in ['created_at', 'updated_at'] and a.type == datetime]
    if len(timestamp_attrs) == 2:
        storage = TimestampedStorage(storage)
    return storage
