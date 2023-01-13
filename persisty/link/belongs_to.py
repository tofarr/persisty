from dataclasses import dataclass
from typing import Optional, ForwardRef, List

import typing_inspect
from marshy.types import ExternalItemType
from schemey.schema import str_schema
from servey.security.authorization import Authorization

from persisty.attr.attr import Attr, DEFAULT_PERMITTED_FILTER_OPS
from persisty.attr.attr_type import AttrType
from persisty.link.linked_store_abc import LinkedStoreABC
from persisty.link.on_delete import OnDelete
from persisty.secured.secured_store_factory_abc import SecuredStoreFactoryABC

from typing import Generic, TypeVar

T = TypeVar('T')


class BelongsToCallable(Generic[T]):

    def __init__(self, key: str, store_factory: SecuredStoreFactoryABC):
        self.key = key
        self.store_factory = store_factory

    def __call__(self, authorization: Optional[Authorization] = None) -> Optional[T]:
        store = self.store_factory.create(authorization)
        item = store.read(self.key)
        return item


@dataclass
class BelongsTo(LinkedStoreABC, Generic[T]):
    name: Optional[str] = None
    key_attr_name: Optional[str] = None
    optional: Optional[bool] = None
    on_delete: OnDelete = OnDelete.BLOCK

    def get_name(self) -> str:
        return self.name

    def get_linked_type(self, forward_ref_ns: str) -> ForwardRef:
        return ForwardRef(forward_ref_ns + '.' + self.get_linked_store_name().title().replace('_', ''))

    # def batch_call(self, items: List, authorization: Optional[Authorization] = None) -> List[Optional[T]]:
    #    if not items:
    #        return []
    #    keys = [getattr(i, self.key_attr_name) for i in items]
    #    self.get_linked_store().read_batch(keys)

    def __set_name__(self, owner, name):
        self.name = name
        if self.key_attr_name is None:
            self.key_attr_name = f"{name}_id"

    def __get__(self, obj, obj_type) -> BelongsToCallable[T]:
        return BelongsToCallable(
            key=getattr(obj, self.key_attr_name),
            store_factory=self.get_linked_store_factory()
        )

    def update_attrs(self, attrs: List[Attr]):
        for attr in attrs:
            if attr.name == self.key_attr_name:
                return
        attrs.append(
            Attr(
                self.key_attr_name,
                AttrType.STR,
                str_schema(),
                sortable=False,
                permitted_filter_ops=DEFAULT_PERMITTED_FILTER_OPS,
            )
        )

    def update_json_schema(self, json_schema: ExternalItemType):
        id_attr_schema = json_schema.get("properties").get(self.key_attr_name)
        id_attr_schema["persistyBelongsTo"] = self.linked_store_name

        id_attr_schema = json_schema.get("properties").get(self.key_attr_name)
        id_attr_schema["persistyBelongsTo"] = self.linked_store_name
