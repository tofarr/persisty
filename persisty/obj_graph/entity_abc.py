import dataclasses
from abc import ABC
from typing import TypeVar, Generic, Iterator, Optional, ForwardRef, Union

from persisty.attr.attr import attrs_from_class
from persisty.attr.attr_abc import AttrABC
from persisty.deferred.deferred_resolution_set import DeferredResolutionSet
from persisty.errors import PersistyError
from persisty.item_filter.item_filter_abc import ItemFilterABC
from persisty.obj_graph.entity_config import EntityConfig
from persisty.obj_graph.selections import Selections
from persisty.page import Page
from persisty.persisty_context_abc import get_default_persisty_context
from persisty.storage.storage_filter import StorageFilter
from persisty.storage.storage_meta import StorageMeta

T = TypeVar('T')
U = Union[ForwardRef('EntityABC'), T]


class EntityABC(ABC, Generic[T]):
    __entity_config__: EntityConfig[T]
    __local_values__: T
    __remote_values__: T

    def __init_subclass__(cls, **kwargs):
        if getattr(cls, '__entity_config__', None) is None:
            item_class = next(c for c in cls.mro() if c != EntityABC)
            # noinspection PyUnresolvedReferences
            if not dataclasses.is_dataclass(item_class) or item_class.__dataclass_params__.frozen:
                raise ValueError(f'non_frozen_dataclass_required:{item_class}')
            item_attrs = attrs_from_class(item_class)
            relational_attrs = tuple(a for a in (c.__dict__.values() for c in cls.mro()) if isinstance(a, AttrABC))
            persisty_context = get_default_persisty_context()
            storage = persisty_context.get_storage(item_class)
            # noinspection PyTypeChecker
            cls.__entity_config__ = EntityConfig(item_class, item_attrs, relational_attrs, storage)
            for attr in item_attrs:
                setattr(cls, attr.name, attr)

    def __init__(self, **kwargs):
        self.__local_values__ = kwargs.get('__local_values__') or self.__entity_config__.item_class()
        self.__remote_values__ = kwargs.get('__remote_values__') or dataclasses.MISSING
        for k, v in kwargs.items():
            if not k.startswith('__'):
                setattr(self, k, v)

    @classmethod
    def get_storage(cls):
        return cls.__entity_config__.storage

    def get_key(self):
        key = self.get_storage().meta.key_config.get_key(self.__local_values__)
        return key

    def get_meta(self):
        meta = self.get_storage().meta
        return StorageMeta(
            name=self.__name__,
            attrs=list(self.__entity_config__.attrs),
            key_config=meta.key_config,
            access_control=meta.access_control,
            cache_control=meta.cache_control
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
        for attr in self.__entity_config__.relational_attrs:
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
        for attr in self.__entity_config__.relational_attrs:
            attr.before_create(self)
        storage = self.get_storage()
        key = storage.create(self.__local_values__)
        self.__entity_config__.storage.meta.key_config.set_key(self.__local_values__, key)
        self.__remote_values__ = dataclasses.replace(self.__local_values__)
        for attr in self.__entity_config__.relational_attrs:
            attr.after_create(self)

    def update(self):
        for attr in self.__entity_config__.relational_attrs:
            attr.before_update(self)
        storage = self.get_storage()
        storage.update(self)
        self.__remote_values__ = dataclasses.replace(self.__local_values__)
        for attr in self.__entity_config__.relational_attrs:
            attr.after_update(self)

    def destroy(self):
        for attr in self.__entity_config__.relational_attrs:
            attr.before_update(self)
        storage = self.get_storage()
        key = storage.meta.key_config.get_key(self.__local_values__)
        if not key:
            return False
        destroyed = storage.destroy(key)
        if not destroyed:
            return False
        self.__remote_values__ = None
        for attr in self.__entity_config__.relational_attrs:
            attr.before_update(self)
        return True

    def patch_from(self, other: U):
        for attr in self.__entity_config__.attrs:
            value = getattr(other, attr.name)
            if value is not dataclasses.MISSING:
                setattr(self, attr.name, value)

    def resolve_all(self,
                    selections: Optional[Selections],
                    deferred_resolutions: Optional[DeferredResolutionSet] = None):
        if selections is None:
            return
        local_deferred_resolutions = DeferredResolutionSet() if deferred_resolutions is None else deferred_resolutions
        for attr in self.__entity_config__.relational_attrs:
            sub_selections = selections.get_selections(attr.name)
            if sub_selections:
                attr.resolve(self, sub_selections, deferred_resolutions)
        if deferred_resolutions is None:
            local_deferred_resolutions.resolve()

    def unresolve_all(self):
        for attr in self.__entity_config__.relational_attrs:
            attr.unresolve(self)

    def get_cache_header(self, selections: Optional[Selections]):
        cache_header = self.get_storage().meta.cache_control.get_cache_header(self.__local_values__)
        if selections is not None:
            cache_header = cache_header.combine_with(self._get_relational_cache_headers(selections))
        return cache_header

    def _get_relational_cache_headers(self, selections: Selections):
        for attr in self.__entity_config__.relational_attrs:
            sub_selections = selections.get_selections(attr.name)
            yield from attr.get_cache_headers(self, sub_selections)
