from dataclasses import dataclass, field
from typing import Generic, Type, Iterable, Iterator, TypeVar, Optional

from persisty.attr.attr import Attr
from persisty.attr.attr_abc import AttrABC
from persisty.storage.storage_abc import StorageABC

T = TypeVar('T')


@dataclass(frozen=True)
class EntityConfig(Generic[T]):
    item_class: Type[T]
    item_attrs: Iterable[Attr]
    relational_attrs: Iterable[AttrABC]
    storage: StorageABC[T]
    filter_class: Optional[Type] = None
    filter_attrs: Iterable[Attr] = field(default_factory=tuple)

    @property
    def attrs(self) -> Iterator[AttrABC]:
        yield from self.item_attrs
        yield from self.relational_attrs
