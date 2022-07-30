from __future__ import annotations
from typing import Generic, TypeVar, Type

from dataclasses import dataclass, field

from marshy import get_default_context
from marshy.marshaller_context import MarshallerContext
from marshy.types import ExternalItemType
from schemey import SchemaContext

from persisty.search_filter.search_filter_factory import SearchFilterFactoryABC
from persisty.search_order.search_order_factory import SearchOrderFactoryABC
from persisty.storage.storage_meta import StorageMeta

T = TypeVar("T")
F = TypeVar("F", bound=SearchFilterFactoryABC)
S = TypeVar("S", bound=SearchOrderFactoryABC)
C = TypeVar("C")
U = TypeVar("U")


@dataclass(frozen=True)
class ObjStorageMeta(Generic[T, F, S, C, U]):
    """Storage meta for object storage"""

    storage_meta: StorageMeta
    item_type: Type[T]
    search_filter_factory_type: Type[F]
    search_order_factory_type: Type[S]
    create_input_type: Type[C]
    update_input_type: Type[U]
    marshaller_context: MarshallerContext = field(default_factory=get_default_context)

    def load_item(self, item: ExternalItemType) -> T:
        loaded = self.item_marshaller.load(item)
        return loaded

    @property
    def item_marshaller(self):
        return self.marshaller_context.get_marshaller(self.item_type)

    def dump_create_input(self, create_input: C) -> ExternalItemType:
        dumped = self.create_input_marshaller.dump(create_input)
        return dumped

    @property
    def create_input_marshaller(self):
        return self.marshaller_context.get_marshaller(self.create_input_type)

    def dump_update_input(self, update_input: C) -> ExternalItemType:
        dumped = self.update_input_marshaller.dump(update_input)
        return dumped

    @property
    def update_input_marshaller(self):
        return self.marshaller_context.get_marshaller(self.update_input_type)


def build_obj_storage_meta(
    storage_meta: StorageMeta, schema_context: SchemaContext
) -> ObjStorageMeta:
    pass
