from dataclasses import dataclass
from typing import Dict, ForwardRef, Generic, List, Optional, TypeVar

import marshy
from marshy.types import ExternalItemType
from schemey import schema_from_type
from servey.security.authorization import Authorization

from persisty.attr.attr import Attr, DEFAULT_PERMITTED_FILTER_OPS
from persisty.attr.attr_type import AttrType
from persisty.attr.generator.default_value_generator import DefaultValueGenerator
from persisty.link.inbound_link import InboundLink
from persisty.link.linked_store_abc import LinkedStoreABC
from persisty.link.on_delete import OnDelete
from persisty.store_meta import StoreMeta

T = TypeVar("T")


class BelongsToCallable(Generic[T]):
    def __init__(self, key: str, store_meta: StoreMeta):
        self.key = key
        self.store_meta = store_meta

    def __call__(self, authorization: Optional[Authorization] = None) -> Optional[T]:
        store = self.store_meta.create_secured_store(authorization)
        item = store.read(self.key)
        return item


@dataclass
class BelongsTo(LinkedStoreABC, Generic[T]):
    name: Optional[str] = None
    key_attr_name: Optional[str] = None
    optional: Optional[bool] = None
    on_delete: OnDelete = OnDelete.IGNORE

    def get_name(self) -> str:
        return self.name

    def get_linked_type(self, forward_ref_ns: str) -> ForwardRef:
        return ForwardRef(
            forward_ref_ns + "." + self.get_linked_store_name().title().replace("_", "")
        )

    async def batch_call(
        self, keys: List, authorization: Optional[Authorization] = None
    ) -> List[Optional[T]]:
        if not keys:
            return []
        result = self.get_linked_store(authorization).read_batch(keys)
        return result

    def arg_extractor(self, obj):
        return [str(getattr(obj, self.key_attr_name))]

    def __set_name__(self, owner, name):
        self.name = name
        if self.key_attr_name is None:
            self.key_attr_name = f"{name}_id"
        if self.linked_store_type is None:
            self.linked_store_type = owner.__dict__["__annotations__"][name]

    def __get__(self, obj, obj_type) -> BelongsToCallable[T]:
        return BelongsToCallable(
            key=getattr(obj, self.key_attr_name),
            store_meta=self.get_linked_store_meta(),
        )

    def update_attrs(self, attrs_by_name: Dict[str, Attr]):
        if self.key_attr_name in attrs_by_name:
            return
        type_ = Optional[str] if self.optional else str
        schema = schema_from_type(type_)
        create_generator = DefaultValueGenerator(None) if self.optional else None
        attrs_by_name[self.key_attr_name] = Attr(
            self.key_attr_name,
            AttrType.STR,
            schema,
            sortable=False,
            create_generator=create_generator,
            permitted_filter_ops=DEFAULT_PERMITTED_FILTER_OPS,
        )

    def update_json_schema(self, json_schema: ExternalItemType):
        id_attr_schema = json_schema.get("properties").get(self.key_attr_name)
        if not id_attr_schema:
            return
        id_attr_schema["persistyBelongsTo"] = {
            "linked_store_name": self.get_linked_store_name(),
            "on_delete": marshy.dump(self.on_delete),
        }

    def get_inbound_links(self, store_meta: StoreMeta) -> List[InboundLink]:
        return [InboundLink(store_meta, self.key_attr_name, self.on_delete)]
