from dataclasses import dataclass, field, fields
from enum import Enum
from typing import (
    Optional,
    Tuple,
    Type,
    TypeVar,
    Dict,
    Set,
    Iterable,
    Callable,
    Union,
    FrozenSet,
)

from marshy.factory.dataclass_marshaller_factory import dataclass_marshaller
from marshy.marshaller_context import MarshallerContext
from schemey import SchemaContext, Schema
from servey.cache_control.cache_control_abc import CacheControlABC
from servey.cache_control.secure_hash_cache_control import SecureHashCacheControl
from servey.security.authorization import Authorization

from persisty.attr.attr import Attr
from persisty.index.index_abc import IndexABC
from persisty.key_config.attr_key_config import ATTR_KEY_CONFIG
from persisty.key_config.key_config_abc import KeyConfigABC
from persisty.link.link_abc import LinkABC
from persisty.search_filter.exclude_all import EXCLUDE_ALL
from persisty.security.store_access import ALL_ACCESS, StoreAccess
from persisty.security.store_security_abc import StoreSecurityABC
from persisty.servey.action_factory_abc import ActionFactoryABC

from persisty.util import to_camel_case
from persisty.util.undefined import UNDEFINED

T = TypeVar("T")
_StoreFactoryABC = "persisty.security.store_factory_abc.StoreFactoryABC"
_StoreABC = "persisty.security.store_abc.StoreABC"


def _default_store_factory():
    from persisty.factory.store_factory import StoreFactory

    return StoreFactory()


def _default_action_factory():
    from persisty.servey.action_factory import ActionFactory

    return ActionFactory()


def _default_store_security():
    from persisty.security.store_security import UNSECURED

    return UNSECURED


# pylint: disable=R0902
@dataclass
class StoreMeta:
    """
    Metadata object for a store. Contains info on the type of data being stored.
    """

    name: str
    attrs: Tuple[Attr, ...]
    key_config: KeyConfigABC = ATTR_KEY_CONFIG
    store_access: StoreAccess = ALL_ACCESS
    store_security: StoreSecurityABC = field(default_factory=_default_store_security)
    cache_control: CacheControlABC = SecureHashCacheControl()
    batch_size: int = 100
    description: Optional[str] = None
    links: Tuple[LinkABC, ...] = tuple()
    indexes: Tuple[IndexABC, ...] = tuple()
    label_attr_names: Tuple[str, ...] = tuple()
    summary_attr_names: Tuple[str, ...] = tuple()
    # pylint: disable=E3701
    store_factory: _StoreFactoryABC = field(default_factory=_default_store_factory)
    # pylint: disable=E3701
    action_factory: ActionFactoryABC = (field(default_factory=_default_action_factory),)
    class_functions: Tuple[Callable, ...] = tuple()

    def get_stored_dataclass(self) -> Type:
        return self._get_dataclass(
            "_stored_dataclass",
            self.name.title().replace("_", ""),
            self.attrs,
            self.links,
        )

    def get_read_dataclass(self) -> Type:
        attrs = [a for a in self.attrs if a.readable]
        return self._get_dataclass(
            "_read_dataclass", self.name.title().replace("_", ""), attrs, self.links
        )

    def get_create_dataclass(self) -> Type:
        required_attr_names = {
            a.name for a in self.attrs if a.creatable and not a.create_generator
        }
        attrs = [a for a in self.attrs if a.creatable]
        return self._get_dataclass(
            "_create_dataclass",
            self.name.title().replace("_", "") + "Create",
            attrs,
            None,
            required_attr_names,
        )

    def get_update_dataclass(self) -> Type:
        required_attr_names = self.key_config.get_key_attrs()
        attrs = [a for a in self.attrs if a.updatable]
        return self._get_dataclass(
            "_update_dataclass",
            self.name.title().replace("_", "") + "Update",
            attrs,
            None,
            required_attr_names,
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

    # pylint: disable=R0913
    def _get_dataclass(
        self,
        attr_name: str,
        name: str,
        attrs: Iterable[Attr],
        links: Optional[Tuple[LinkABC, ...]] = None,
        required_attr_names: Union[type(None), Set[str], FrozenSet[str]] = None,
    ) -> Optional[Type]:
        result = getattr(self, attr_name, None)
        if result is None:
            annotations = {}
            params = {"__annotations__": annotations, "__stored_type__": attr_name}
            for attr in attrs:
                if required_attr_names and attr.name in required_attr_names:
                    params[attr.name] = attr.to_field(True)
                    annotations[attr.name] = attr.schema.python_type
            for attr in attrs:
                if not required_attr_names or attr.name not in required_attr_names:
                    params[attr.name] = attr.to_field(False)
                    annotations[attr.name] = attr.schema.python_type
            for class_function in self.class_functions:
                params[getattr(class_function, "__name__", None)] = class_function
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
                # noinspection PyTypeChecker
                result = dataclass(type(name, tuple(), params))
            else:
                result = None
            setattr(self, attr_name, result)
        return result

    def create_store(self) -> _StoreABC:
        store = self.store_factory.create(self)
        return store

    def create_api_meta(self) -> _StoreABC:
        result = self.store_security.get_api_meta(self)
        return result

    def create_secured_store(self, authorization: Optional[Authorization]) -> _StoreABC:
        store = self.store_security.get_secured(self.create_store(), authorization)
        return store


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


# pylint: disable=W0613
# noinspection PyDecorator,PyUnusedLocal
@classmethod
def _schema_factory(
    cls, context: SchemaContext, path: str, ref_schemas: Dict[Type, Schema]
):
    """Override the default schemey behavior and do not include defaults with these generated dataclasses"""
    # noinspection PyDataclass
    schema = {
        "name": cls.__name__,
        "type": "object",
        "properties": {f.name: f.metadata.get("schemey").schema for f in fields(cls)},
        "additionalProperties": False,
    }
    required = [f.name for f in fields(cls) if f.default is not UNDEFINED]
    if required:
        schema["required"] = required
    if cls.__doc__:
        schema["description"] = cls.__doc__.strip()
    store_meta = get_meta(cls)
    schema["persistyStored"] = {
        "store_name": store_meta.name,
        "creatable": store_meta.store_access.create_filter is not EXCLUDE_ALL,
        "label_attr_names": list(store_meta.label_attr_names),
        "summary_attr_names": list(store_meta.summary_attr_names),
    }
    for link in store_meta.links:
        link.update_json_schema(schema)
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
