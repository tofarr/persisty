import dataclasses
from typing import Generic, Type, Iterable, Iterator, TypeVar

from persisty.attr.attr import Attr
from persisty.attr.attr_abc import AttrABC
from persisty.storage.storage_abc import StorageABC

T = TypeVar('T')


@dataclasses.dataclass(frozen=True)
class EntityConfig(Generic[T]):
    item_class: Type[T]
    item_attrs: Iterable[Attr]
    relational_attrs: Iterable[AttrABC]
    storage: StorageABC[T]

    @property
    def attrs(self) -> Iterator[AttrABC]:
        yield from self.item_attrs
        yield from self.relational_attrs
