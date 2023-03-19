import dataclasses
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Tuple, Type, Iterator, TypeVar, Dict

from marshy.factory.dataclass_marshaller_factory import dataclass_marshaller
from marshy.marshaller_context import MarshallerContext
from schemey import SchemaContext, Schema
from servey.cache_control.cache_control_abc import CacheControlABC
from servey.cache_control.secure_hash_cache_control import SecureHashCacheControl

from persisty.attr.attr import Attr
from persisty.index import Index
from persisty.key_config.attr_key_config import ATTR_KEY_CONFIG
from persisty.key_config.key_config_abc import KeyConfigABC
from persisty.link.link_abc import LinkABC
from persisty.store_access import StoreAccess, ALL_ACCESS

from persisty.util import to_camel_case
from persisty.util.undefined import UNDEFINED

T = TypeVar("T")


@dataclass
class StoreMeta:
    """
    Metadata object for a store. Contains info on the type of data being stored.
    """

    name: str
    attrs: Tuple[Attr, ...]
    key_config: KeyConfigABC = ATTR_KEY_CONFIG
    store_access: StoreAccess = ALL_ACCESS
    cache_control: CacheControlABC = SecureHashCacheControl()
    batch_size: int = 100
    description: Optional[str] = None
    links: Tuple[LinkABC, ...] = tuple()
    indexes: Tuple[Index, ...] = tuple()

    def get_stored_dataclass(self) -> Type:
        return self._get_dataclass(
            "_stored_dataclass",
            self.name.title().replace("_", ""),
            iter(self.attrs),
            self.links,
        )

    def get_read_dataclass(self) -> Type:
        attrs = (a for a in self.attrs if a.readable)
        return self._get_dataclass(
            "_read_dataclass", self.name.title().replace("_", ""), attrs, self.links
        )

    def get_create_dataclass(self) -> Type:
        attrs = (a for a in self.attrs if a.creatable)
        return self._get_dataclass(
            "_create_dataclass",
            self.name.title().replace("_", "") + "CreateInput",
            attrs,
        )

    def get_update_dataclass(self) -> Type:
        attrs = (a for a in self.attrs if a.updatable)
        return self._get_dataclass(
            "_update_dataclass",
            self.name.title().replace("_", "") + "UpdateInput",
            attrs,
        )

    def get_search_filter_factory_dataclass(self) -> Optional[Type]:
        search_filter_factory_dataclass = getattr(
            self, "_search_filter_factory_dataclass", UNDEFINED
        )
        if search_filter_factory_dataclass is UNDEFINED:
            from persisty.search_filter.search_filter_factory import (
                search_filter_dataclass_for,
            )

            search_filter_factory_dataclass = search_filter_dataclass_for(self)
            setattr(
                self,
                "_search_filter_factory_dataclass",
                search_filter_factory_dataclass,
            )
        return search_filter_factory_dataclass

    def get_sort_order_factory_dataclass(self) -> Optional[Type]:
        search_order_factory_dataclass = getattr(
            self, "_search_order_factory_dataclass", UNDEFINED
        )
        if search_order_factory_dataclass is UNDEFINED:
            from persisty.search_order.search_order_factory import (
                search_order_dataclass_for,
            )

            search_order_factory_dataclass = search_order_dataclass_for(self)
            setattr(
                self, "_search_order_factory_dataclass", search_order_factory_dataclass
            )
        return search_order_factory_dataclass

    def get_sortable_attrs_as_enum(self) -> Optional[Enum]:
        result = getattr(self, "_sortable_attrs", UNDEFINED)
        if result is UNDEFINED:
            attrs = {f.name: f.name for f in self.attrs if f.sortable}
            if attrs:
                result = Enum(f"{to_camel_case(self.name)}Sortable", attrs)
            else:
                result = None
            setattr(self, "_sortable_attrs", result)
        return result

    def _get_dataclass(
        self,
        attr_name: str,
        name: str,
        attrs: Iterator[Attr],
        links: Optional[Tuple[LinkABC, ...]] = None,
    ) -> Optional[Type]:
        result = getattr(self, attr_name, None)
        if result is None:
            annotations = {}
            params = {"__annotations__": annotations, "__stored_type__": attr_name}
            for attr in attrs:
                params[attr.name] = attr.to_field()
                annotations[attr.name] = attr.schema.python_type
            if annotations:
                if links:
                    for link in links:
                        params[link.get_name()] = link
                if self.description:
                    params["__doc__"] = self.description
                params["__persisty_store_meta__"] = self
                params["__schema_factory__"] = _schema_factory
                params["__marshaller_factory__"] = _marshaller_factory
                params["__eq__"] = _eq
                result = dataclass(type(name, tuple(), params))
            else:
                result = None
            setattr(self, attr_name, result)
        return result


def get_meta(type_: Type) -> Optional[StoreMeta]:
    meta = getattr(type_, "__persisty_store_meta__", None)
    return meta


def get_stored_dataclass(type_: Type[T]) -> Type[T]:
    # noinspection PyTypeChecker
    return get_meta(type_).get_stored_dataclass()


def get_read_dataclass(type_: Type[T]) -> Type[T]:
    # noinspection PyTypeChecker
    return get_meta(type_).get_read_dataclass()


def get_create_dataclass(type_: Type[T]) -> Type[T]:
    # noinspection PyTypeChecker
    return get_meta(type_).get_create_dataclass()


def get_update_dataclass(type_: Type[T]) -> Type[T]:
    # noinspection PyTypeChecker
    return get_meta(type_).get_update_dataclass()


# noinspection PyDecorator,PyUnusedLocal
@classmethod
def _schema_factory(
    cls, context: SchemaContext, path: str, ref_schemas: Dict[Type, Schema]
):
    """Override the default schemey behavior and do not include defaults with these generated dataclasses"""
    # noinspection PyDataclass
    schema = {
        "type": "object",
        "properties": {
            f.name: f.metadata.get("schemey").schema for f in dataclasses.fields(cls)
        },
        "additionalProperties": False,
    }
    if cls.__doc__:
        schema["description"] = cls.__doc__.strip()
    schema = Schema(schema, cls)
    return schema


# noinspection PyDecorator
@classmethod
def _marshaller_factory(cls, marshaller_context: MarshallerContext):
    return dataclass_marshaller(
        cls, marshaller_context, exclude_dumped_values=(UNDEFINED,)
    )


def _eq(self, other):
    meta: StoreMeta = get_meta(self)
    for attr in meta.attrs:
        a = getattr(self, attr.name, UNDEFINED)
        b = getattr(other, attr.name, UNDEFINED)
        if a != b:
            return False
    return True
