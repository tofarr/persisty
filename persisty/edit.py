from dataclasses import dataclass
from typing import TypeVar, Generic, Optional

from persisty.edit_type import EditType

T = TypeVar('T')


@dataclass(frozen=True)
class Edit(Generic[T]):
    edit_type: EditType
    key: Optional[str] = None
    value: Optional[T] = None

    def __post_init__(self):
        if self.edit_type in (EditType.CREATE, EditType.UPDATE) and self.key is not None:
            raise ValueError('key_specified_for_upsert')
        if self.edit_type == EditType.DESTROY and self.value is not None:
            raise ValueError('value_specified_for_destroy')
