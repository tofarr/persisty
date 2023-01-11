from dataclasses import dataclass
from enum import Enum
from typing import Optional, Tuple, Type, Iterator, Union, TypeVar

from servey.cache_control.cache_control_abc import CacheControlABC
from servey.cache_control.secure_hash_cache_control import SecureHashCacheControl

from aaaa.attr.attr import Attr
from aaaa.batch_edit import batch_edit_dataclass_for
from aaaa.batch_edit_result import batch_edit_result_dataclass_for
from aaaa.index import Index
from aaaa.key_config.attr_key_config import ATTR_KEY_CONFIG
from aaaa.key_config.key_config_abc import KeyConfigABC
from aaaa.link.link_abc import LinkABC
from aaaa.result_set import ResultSet, result_set_dataclass_for
from aaaa.store_access import StoreAccess, ALL_ACCESS

from aaaa.entity import EntityABC
from aaaa.util import to_camel_case
from aaaa.util.undefined import UNDEFINED

T = TypeVar('T')


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
        return self._get_dataclass('_stored_dataclass', self.name, iter(self.attrs))

    def get_read_dataclass(self) -> Type:
        attrs = (a for a in self.attrs if a.readable)
        return self._get_dataclass('_read_dataclass', self.name, attrs)

    def get_create_dataclass(self) -> Type:
        attrs = (a for a in self.attrs if a.creatable)
        return self._get_dataclass('_create_dataclass', self.name+'CreateInput', attrs)

    def get_update_dataclass(self) -> Type:
        attrs = (a for a in self.attrs if a.updatable)
        return self._get_dataclass('_update_dataclass', self.name+'UpdateInput', attrs)

    def get_search_filter_factory_dataclass(self) -> Optional[Type]:
        search_filter_factory_dataclass = getattr(self, '_search_filter_factory_dataclass', UNDEFINED)
        if search_filter_factory_dataclass is UNDEFINED:
            from aaaa.search_filter.search_filter_factory import search_filter_dataclass_for
            search_filter_factory_dataclass = search_filter_dataclass_for(self)
            setattr(self, '_search_filter_factory_dataclass', search_filter_factory_dataclass)
        return search_filter_factory_dataclass

    def get_sort_order_factory_dataclass(self) -> Optional[Type]:
        search_order_factory_dataclass = getattr(self, '_search_order_factory_dataclass', UNDEFINED)
        if search_order_factory_dataclass is UNDEFINED:
            from aaaa.search_order.search_order_factory import search_order_dataclass_for
            search_order_factory_dataclass = search_order_dataclass_for(self)
            setattr(self, '_search_order_factory_dataclass', search_order_factory_dataclass)
        return search_order_factory_dataclass

    def get_sortable_attrs_as_enum(self) -> Optional[Enum]:
        result = getattr(self, '_sortable_attrs', UNDEFINED)
        if result is UNDEFINED:
            attrs = {f.name: f.name for f in self.attrs if f.sortable}
            if attrs:
                result = Enum(f"{to_camel_case(self.name)}Sortable", attrs)
            else:
                result = None
            setattr(self, '_sortable_attrs', result)
        return result

    def get_result_set_dataclass(self) -> Type[ResultSet]:
        result = getattr(self, '_result_set', None)
        if result is None:
            # noinspection PyTypeChecker
            result = result_set_dataclass_for(self.get_read_dataclass())
            setattr(self, '_result_set', result)
        return result

    def get_batch_edit_dataclass(self):
        result = getattr(self, '_batch_edit', None)
        if result is None:
            # noinspection PyTypeChecker
            result = batch_edit_dataclass_for(
                self.name+'BatchEdit',
                self.get_create_dataclass(),
                self.get_update_dataclass()
            )
            setattr(self, '_batch_edit', result)
        return result

    def get_batch_edit_result_dataclass(self):
        result = getattr(self, '_batch_edit_result', None)
        if result is None:
            # noinspection PyTypeChecker
            result = batch_edit_result_dataclass_for(self.get_batch_edit_dataclass())
            setattr(self, '_batch_edit_result', result)
        return result

    def get_entity_class(self) -> EntityABC:
        assert False

    def _get_dataclass(self, attr_name: str, name: str, attrs: Iterator[Attr]) -> Optional[Type]:
        result = getattr(self, attr_name, None)
        if result is None:
            annotations = {}
            fields = {
                '__annotations__': annotations
            }
            for attr in attrs:
                fields[attr.name] = attr.to_field()
                annotations[attr.name] = attr.schema.python_type
            if annotations:
                fields['__persisty_store_meta__'] = self
                result = dataclass(type(name, tuple(), fields))
            else:
                result = None
            setattr(self, attr_name, result)
        return result


def get_store_meta(type_: Type) -> Optional[StoreMeta]:
    meta = getattr(type_, '__persisty_store_meta__', None)
    return meta


def get_entity_class(type_: Type[T]) -> Type[Union[T, EntityABC]]:
    return get_store_meta(type_).get_entity_class()


def get_stored_dataclass(type_: Type[T]) -> Type[T]:
    # noinspection PyTypeChecker
    return get_store_meta(type_).get_stored_dataclass()


def get_read_dataclass(type_: Type[T]) -> Type[T]:
    # noinspection PyTypeChecker
    return get_store_meta(type_).get_read_dataclass()


def get_create_dataclass(type_: Type[T]) -> Type[T]:
    # noinspection PyTypeChecker
    return get_store_meta(type_).get_create_dataclass()


def get_update_dataclass(type_: Type[T]) -> Type[T]:
    # noinspection PyTypeChecker
    return get_store_meta(type_).get_update_dataclass()
