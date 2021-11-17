from collections import Iterable
from dataclasses import fields, dataclass
from typing import TypeVar, Type, List

import typing_inspect
from marshy.marshaller.marshaller_abc import MarshallerABC
from marshy.marshaller_context import MarshallerContext
from marshy.types import ExternalItemType

from persisty.obj_graph.old_entity_abc import EntityABC

T = TypeVar('T', bound=EntityABC)


@dataclass(frozen=True)
class EntityMarshaller(MarshallerABC[T]):
    context: MarshallerContext

    def load(self, item: ExternalItemType) -> T:
        init_field_values = {f.name: self.context.load(f.type, item.get(f.name))
                             for f in fields(self.marshalled_type) if f.name in item and f.init}
        entity = self.marshalled_type(**init_field_values)
        return entity

    def dump(self, item: T) -> ExternalItemType:
        dumped = {a.name: self.context.dump(getattr(item, a.name), a.type) for a in item.__entity_config__.attrs}
        return dumped
