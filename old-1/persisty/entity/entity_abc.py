import dataclasses
from abc import ABC
from typing import TypeVar, Generic, Iterator, Optional, ForwardRef, Union, Type

from marshy.utils import resolve_forward_refs
from schemey.object_schema import ObjectSchema
from schemey.property_schema import PropertySchema

from persisty.attr.attr import attrs_from_class
from persisty.attr.attr_abc import AttrABC
from persisty.deferred.deferred_resolution_set import DeferredResolutionSet
from persisty.entity.entity_config import EntityConfig
from persisty.entity.entity_meta import EntityMeta
from persisty.entity.selections import Selections
from persisty.errors import PersistyError
from persisty.item_filter.item_filter_abc import ItemFilterABC
from persisty.page import Page
from persisty.storage.storage_context_abc import get_default_storage_context
from persisty.storage.storage_filter import StorageFilter

T = TypeVar('T')
U = Union[ForwardRef('EntityABC'), T]


class EntityABC(ABC, Generic[T]):
    __entity_config__: EntityConfig[T]
    __filter_class__: Type
    __local_values__: T
    __remote_values__: T

    def __init_subclass__(cls, **kwargs):
        if getattr(cls, '__entity_config__', None) is None:
            item_class = next(c for c in cls.mro() if c not in (EntityABC, ABC, Generic, cls))
            # noinspection PyUnresolvedReferences
            if not dataclasses.is_dataclass(item_class) or item_class.__dataclass_params__.frozen:
                raise ValueError(f'non_frozen_dataclass_required:{item_class}')
            item_attrs = attrs_from_class(item_class)
            relational_attrs = tuple(cls._get_relational_attrs())
            filter_class = getattr(cls, '__filter_class__', None)
            filter_attrs = tuple()
            if filter_class:
                filter_class = resolve_forward_refs(filter_class)
                filter_attrs = attrs_from_class(filter_class)
            # noinspection PyTypeChecker
            cls.__entity_config__ = EntityConfig(item_class, item_attrs, relational_attrs, filter_class, filter_attrs)
            for attr in item_attrs:
                setattr(cls, attr.name, attr)
            # We force the __init__ method to be the one from entity abc - this means that code completion usually
            # thinks it is the one from the dataclass, but we have some secret sauce under the hood
            # noinspection PyTypeChecker
            cls.__init__ = EntityABC.__init__

    @classmethod
    def _get_relational_attrs(cls):
        for c in cls.mro():
            dict_ = c.__dict__
            for a in dict_.values():
                if isinstance(a, AttrABC):
                    yield a

    def __init__(self, *args, **kwargs):
        self.__local_values__ = kwargs.get('__local_values__') or self.__entity_config__.item_class(*args, **kwargs)
        self.__remote_values__ = kwargs.get('__remote_values__') or dataclasses.MISSING

    @classmethod
    def get_entity_config(cls):
        return cls.__entity_config__

    @classmethod
    def get_storage(cls):
        """ Get storage for this entity from the default context - Override this to provide your own storage object."""
        storage = getattr(cls, '__storage__', None)
        if storage is not None:
            return storage
        context = get_default_storage_context()
        storage = context.get_storage(cls.get_entity_config().item_class)
        return storage

    def get_key(self):
        key = self.get_storage().meta.key_config.get_key(self.__local_values__)
        return key

    def get_meta(self) -> EntityMeta:
        meta = self.get_storage().meta
        return EntityMeta(
            name=self.__name__,
            attrs=list(self.get_entity_config().attrs),
            key_config=meta.key_config,
            access_control=meta.access_control,
            cache_control=meta.cache_control,
            filter_attrs=self.get_entity_config().filter_attrs
        )

    @classmethod
    def _wrap_entities(cls,
                       items: Iterator[T],
                       selections: Optional[Selections],
                       deferred_resolutions: Optional[DeferredResolutionSet]
                       ) -> Iterator[U]:
        if selections is None:
            selections = Selections()
        local_deferred_resolutions = deferred_resolutions or DeferredResolutionSet()
        entities = []
        for item in items:
            entity = cls(__local_values__=item, __remote_values__=dataclasses.replace(item))
            entities.append(entity)
            entity.resolve_all(selections, local_deferred_resolutions)
        if not deferred_resolutions:
            local_deferred_resolutions.resolve()
        return iter(entities)

    @classmethod
    def read(cls,
             key: str,
             selections: Optional[Selections] = None,
             deferred_resolutions: Optional[DeferredResolutionSet] = None
             ) -> U:
        storage = cls.get_storage()
        item = storage.read(key)
        if item is None:
            return None
        entities = cls._wrap_entities((item,), selections, deferred_resolutions)
        return next(entities)

    @classmethod
    def read_all(cls,
                 keys: Iterator[str],
                 error_on_missing: bool = True,
                 selections: Optional[Selections] = None,
                 deferred_resolutions: Optional[DeferredResolutionSet] = None
                 ) -> Iterator[U]:
        storage = cls.get_storage()
        items = storage.read_all(keys, error_on_missing)
        entities = cls._wrap_entities(items, selections, deferred_resolutions)
        return entities

    @classmethod
    def search(cls,
               storage_filter: Optional[StorageFilter[T]] = None,
               selections: Optional[Selections] = None,
               deferred_resolutions: Optional[DeferredResolutionSet] = None):
        storage = cls.get_storage()
        items = storage.search(storage_filter)
        entities = cls._wrap_entities(items, selections, deferred_resolutions)
        return entities

    @classmethod
    def count(cls, item_filter: Optional[ItemFilterABC[T]] = None) -> int:
        storage = cls.get_storage()
        count = storage.count(item_filter)
        return count

    @classmethod
    def paged_search(cls,
                     storage_filter: Optional[StorageFilter[T]] = None,
                     page_key: Optional[str] = None,
                     limit: int = 20,
                     selections: Optional[Selections] = None,
                     deferred_resolutions: Optional[DeferredResolutionSet] = None):
        storage = cls.get_storage()
        page = storage.paged_search(storage_filter, page_key, limit)
        entities = cls._wrap_entities(iter(page.items), selections, deferred_resolutions)
        wrapped_page = Page(list(entities), page.next_page_key)
        return wrapped_page

    @property
    def is_existing(self):
        storage = self.get_storage()
        key = self.get_key()
        if key is None:
            return False
        if self.__remote_values__ is dataclasses.MISSING:
            self.__remote_values__ = storage.read(key)
        return bool(self.__remote_values__)

    def load(self):
        storage = self.get_storage()
        item = self.__local_values__ if self.__remote_values__ is dataclasses.MISSING else self.__remote_values__
        key = storage.meta.key_config.get_key(item)
        if key is None:
            raise PersistyError('missing_key')
        loaded = storage.read(key)
        self.__remote_values__ = loaded
        if not loaded:
            raise PersistyError(f'no_such_entity:{key}')
        self.__local_values__ = dataclasses.replace(loaded)

    @property
    def is_save_required(self):
        for attr in self.get_entity_config().relational_attrs:
            if attr.is_save_required(self):
                return True
        return self.__local_values__ != self.__remote_values__

    def save(self):
        if not self.is_save_required:
            return
        if self.is_existing:
            return self.update()
        else:
            return self.create()

    def create(self):
        for attr in self.get_entity_config().relational_attrs:
            attr.before_create(self)
        storage = self.get_storage()
        key = storage.create(self.__local_values__)
        storage.meta.key_config.set_key(self.__local_values__, key)
        self.__remote_values__ = dataclasses.replace(self.__local_values__)
        for attr in self.get_entity_config().relational_attrs:
            attr.after_create(self)

    def update(self):
        for attr in self.get_entity_config().relational_attrs:
            attr.before_update(self)
        storage = self.get_storage()
        storage.update(self)
        self.__remote_values__ = dataclasses.replace(self.__local_values__)
        for attr in self.get_entity_config().relational_attrs:
            attr.after_update(self)

    def destroy(self):
        for attr in self.get_entity_config().relational_attrs:
            attr.before_update(self)
        storage = self.get_storage()
        key = storage.meta.key_config.get_key(self.__local_values__)
        if not key:
            return False
        destroyed = storage.destroy(key)
        if not destroyed:
            return False
        self.__remote_values__ = None
        for attr in self.get_entity_config().relational_attrs:
            attr.before_update(self)
        return True

    def patch_from(self, other: U):
        for attr in self.get_entity_config().attrs:
            value = getattr(other, attr.name)
            if value is not dataclasses.MISSING:
                setattr(self, attr.name, value)

    def resolve_all(self,
                    selections: Optional[Selections],
                    deferred_resolutions: Optional[DeferredResolutionSet] = None):
        if selections is None:
            return
        local_deferred_resolutions = DeferredResolutionSet() if deferred_resolutions is None else deferred_resolutions
        for attr in self.get_entity_config().relational_attrs:
            sub_selections = selections.get_selections(attr.name)
            if sub_selections:
                attr.resolve(self, sub_selections, deferred_resolutions)
        if deferred_resolutions is None:
            local_deferred_resolutions.resolve()

    def unresolve_all(self):
        for attr in self.get_entity_config().relational_attrs:
            attr.unresolve(self)

    def get_cache_header(self, selections: Optional[Selections]):
        cache_header = self.get_storage().meta.cache_control.get_cache_header(self.__local_values__)
        if selections is not None:
            cache_header = cache_header.combine_with(self._get_relational_cache_headers(selections))
        return cache_header

    def _get_relational_cache_headers(self, selections: Selections):
        for attr in self.get_entity_config().relational_attrs:
            sub_selections = selections.get_selections(attr.name)
            yield from attr.get_cache_headers(self, sub_selections)

    @classmethod
    def get_schema(cls) -> ObjectSchema:
        attr_schemas = tuple(PropertySchema(a.name, a.schema) for a in cls.get_entity_config().attrs)
        schema = ObjectSchema(cls, attr_schemas)
        return schema

    def to_item(self) -> T:
        item = dataclasses.replace(self.__local_values__)
        for field in dataclasses.fields(self.get_entity_config().item_class):
            if not field.init:
                value = getattr(self.__local_values__, field.name)
                setattr(item, field.name, value)
        return item
