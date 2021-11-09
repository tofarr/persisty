from collections import Iterable
from dataclasses import fields, dataclass
from typing import TypeVar, Type, List

import typing_inspect
from marshy.marshaller.marshaller_abc import MarshallerABC
from marshy.marshaller_context import MarshallerContext
from marshy.types import ExternalItemType

from persisty.obj_graph.entity_abc import EntityABC

T = TypeVar('T', bound=EntityABC)


@dataclass(frozen=True)
class EntityMarshaller(MarshallerABC[T]):
    context: MarshallerContext

    def load(self, item: ExternalItemType) -> T:
        init_field_values = {f.name: self.context.load(f.type, item.get(f.name))
                             for f in fields(self.marshalled_type) if f.name in item and f.init}
        entity = self.marshalled_type(**init_field_values)
        for f in fields(self.marshalled_type):
            if f.name in item and not f.init:
                value = self.context.load(f.type, item.get(f.name))
                setattr(entity, f.name, value)
        for r in self.marshalled_type.get_resolvers():
            if r.name in item:
                resolved_value = self.context.load(r.resolved_type, item.get(r.name))
                r.__set__(entity, resolved_value)
        return entity

    def dump(self, item: T) -> ExternalItemType:
        field_values = {f.name: self.context.dump(getattr(item, f.name))
                        for f in fields(self.marshalled_type)}
        resolved_values = {r.name: self.context.dump(getattr(item, r.name), self._resolved_type(r.resolved_type))
                           for r in self.marshalled_type.get_resolvers() if r.is_resolved(item)}
        return {**field_values, **resolved_values}

    def _resolved_type(self, type_: Type):
        if typing_inspect.get_origin(type_) == Iterable:
            return List[typing_inspect.get_args(type_)[0]]
        return type_
