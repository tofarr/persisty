from dataclasses import dataclass
from typing import TypeVar, Generic, Optional

from persisty.edit_type import EditType

T = TypeVar('T')


@dataclass(frozen=True)
class Edit(Generic[T]):
    edit_type: EditType
    key: Optional[str] = None
    item: Optional[T] = None

    def __post_init__(self):
        if self.edit_type in (EditType.CREATE, EditType.UPDATE) and self.key is not None:
            raise ValueError('key_specified_for_upsert')
        if self.edit_type == EditType.DESTROY and self.item is not None:
            raise ValueError('item_specified_for_destroy')
        if self.key is None and self.item is None:
            raise ValueError('key_or_item_required')

    @staticmethod
    def create(item: T):
        return Edit[T](EditType.CREATE, item=item)

    @staticmethod
    def update(item: T):
        return Edit[T](EditType.UPDATE, item=item)

    @staticmethod
    def destroy(key: str):
        return Edit[T](EditType.DESTROY, key)
