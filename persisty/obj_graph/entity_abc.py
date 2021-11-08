import dataclasses
from abc import ABC
from typing import Optional, TypeVar, Generic, Union, ForwardRef, Iterator, Set, Type

from persisty.cache_header import CacheHeader
from persisty.item_filter.item_filter_abc import ItemFilterABC
from persisty.obj_graph.deferred.deferred_resolution_set import DeferredResolutionSet
from persisty.obj_graph.resolver.resolver_abc import ResolverABC, NOT_INITIALIZED
from persisty.obj_graph.selection_set import SelectionSet
from persisty.page import Page
from persisty.errors import PersistyError
from schemey.schema_abc import SchemaABC

from persisty.persisty_meta import PersistyMeta
from persisty.search_filter import SearchFilter
from persisty.store.store_abc import StoreABC

T = TypeVar('T')

REMOTE_VALUES_ATTR = '__remote_values__'
ITEMS_ATTR = 'items'
KEY_ATTR = '__key_attr__'
ID = 'id'


class EntityABC(Generic[T], ABC):

    __wrapped_class__: T = None
    __batch_size__: 100
    __filter_class__: Type = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)  # forwards all unused arguments
        self.__remote_values__ = NOT_INITIALIZED

    @classmethod
    def get_resolvers(cls) -> Iterator[ResolverABC]:
        return (e for e in cls.__dict__.values() if isinstance(e, ResolverABC))

    @classmethod
    def get_name(cls):
        return cls._get_wrapped_class().__name__

    @classmethod
    def get_store(cls) -> StoreABC[T]:
        name = cls.get_name()
        if not hasattr(cls, '__persisty_context__'):
            from persisty.persisty_context import get_default_persisty_context
            cls.__persisty_context__ = get_default_persisty_context()
        store = cls.__persisty_context__.get_store(name)
        return store

    @classmethod
    def _get_wrapped_class(cls):
        if not cls.__wrapped_class__:
            cls.__wrapped_class__ = next(s for s in cls.__mro__[1:] if dataclasses.is_dataclass(s))
        return cls.__wrapped_class__

    @classmethod
    def _wrap_entities(cls,
                       items: Iterator[T],
                       selections: Optional[SelectionSet],
                       deferred_resolutions: Optional[DeferredResolutionSet]
                       ):
        if selections is None:
            selections = SelectionSet()
        local_deferred_resolutions = deferred_resolutions or DeferredResolutionSet()
        init_funcs = [f for f in dataclasses.fields(cls._get_wrapped_class()) if f.init]
        non_init_funcs = [f for f in dataclasses.fields(cls._get_wrapped_class()) if not f.init]
        entities = []
        for item in items:
            entity = cls(**{f.name: getattr(item, f.name) for f in init_funcs})
            for f in non_init_funcs:
                object.__setattr__(entity, f.name, getattr(item, f.name))
            object.__setattr__(entity, REMOTE_VALUES_ATTR, item)
            entities.append(entity)
            entity.resolve_all(selections, local_deferred_resolutions)
        if not deferred_resolutions:
            local_deferred_resolutions.resolve()
        return iter(entities)

    @classmethod
    def read(cls,
             key: str,
             selections: Optional[SelectionSet] = None,
             deferred_resolutions: Optional[DeferredResolutionSet] = None
             ) -> Union[ForwardRef('EntityABC'), T]:
        store = cls.get_store()
        item = store.read(key)
        if item is None:
            return None
        entities = cls._wrap_entities((item,), selections, deferred_resolutions)
        return next(entities)

    @classmethod
    def read_all(cls,
                 keys: Iterator[str],
                 error_on_missing: bool = True,
                 selections: Optional[SelectionSet] = None,
                 deferred_resolutions: Optional[DeferredResolutionSet] = None
                 ) -> Iterator[T]:
        store = cls.get_store()
        items = store.read_all(keys, error_on_missing)
        entities = cls._wrap_entities(items, selections, deferred_resolutions)
        return entities

    @classmethod
    def search(cls,
               search_filter: Optional[SearchFilter[T]] = None,
               selections: Optional[SelectionSet] = None,
               deferred_resolutions: Optional[DeferredResolutionSet] = None):
        store = cls.get_store()
        items = store.search(search_filter)
        entities = cls._wrap_entities(items, selections, deferred_resolutions)
        return entities

    @classmethod
    def count(cls, item_filter: Optional[ItemFilterABC[T]] = None) -> int:
        store = cls.get_store()
        count = store.count(item_filter)
        return count

    @classmethod
    def paged_search(cls,
                     search_filter: Optional[SearchFilter[T]] = None,
                     page_key: Optional[str] = None,
                     limit: int = 20,
                     selections: Optional[SelectionSet] = None,
                     deferred_resolutions: Optional[DeferredResolutionSet] = None):
        store = cls.get_store()
        page = store.paged_search(search_filter, page_key, limit)
        entities = cls._wrap_entities(iter(page.items), selections, deferred_resolutions)
        wrapped_page = Page(list(entities), page.next_page_key)
        return wrapped_page

    @property
    def is_save_required(self):
        for resolver in self.get_resolvers():
            if resolver.is_overridden(self):
                return True
        return self != self.__remote_values__

    def get_key(self):
        key = self.get_store().get_key(self)
        return key

    @property
    def is_existing(self):
        store = self.get_store()
        key = store.get_key(self)
        if key is None:
            return False
        if self.__remote_values__ is NOT_INITIALIZED:
            self.__remote_values__ = store.read(key)
        return bool(self.__remote_values__)

    def load(self):
        store = self.get_store()
        key = store.get_key(self if self.__remote_values__ is NOT_INITIALIZED else self.__remote_values__)
        if key is None:
            raise PersistyError('missing_key')
        self.__remote_values__ = store.read(key)
        if not self.__remote_values__:
            raise PersistyError(f'no_such_entity:{key}')
        for f in dataclasses.fields(self._get_wrapped_class()):
            object.__setattr__(self, f.name, getattr(self.__remote_values__, f.name))

    def save(self):
        if not self.is_save_required:
            return
        if self.is_existing:
            return self.update()
        else:
            return self.create()

    def create(self):
        for resolver in self.get_resolvers():
            resolver.before_create(self)
        store = self.get_store()
        key = store.create(self)
        object.__setattr__(self, self._get_key_attr(), key)
        self._build_remote_from_local()
        for resolver in self.get_resolvers():
            resolver.after_create(self)

    def _get_key_attr(self):
        return self.__key_attr__ if hasattr(self, '__key_attr__') else 'id'

    def _build_remote_from_local(self):
        self.__remote_values__ = self.to_item()

    def to_item(self) -> T:
        init_fields = (f for f in dataclasses.fields(self._get_wrapped_class()) if f.init)
        kwargs = {f.name: getattr(self, f.name) for f in init_fields}
        item = self._get_wrapped_class()(**kwargs)
        return item

    def update(self):
        for resolver in self.get_resolvers():
            resolver.before_update(self)
        store = self.get_store()
        store.update(self)
        self._build_remote_from_local()
        for resolver in self.get_resolvers():
            resolver.after_update(self)

    def destroy(self):
        for resolver in self.get_resolvers():
            resolver.before_destroy(self)
        store = self.get_store()
        key = store.get_key(self)
        store.destroy(key)
        self.__remote_values__ = None
        for resolver in self.get_resolvers():
            resolver.after_destroy(self)

    def patch_from(self, other):
        for f in dataclasses.fields(self._get_wrapped_class()):
            value = getattr(other, f.name)
            if value is not dataclasses.MISSING:
                setattr(self, f.name, value)
        for resolver in self.get_resolvers():
            if resolver.is_resolved(other):
                value = getattr(other, resolver.name)
                resolver.__set__(self, value)

    def resolve_all(self,
                    selections: Optional[SelectionSet],
                    deferred_resolutions: Optional[DeferredResolutionSet] = None):
        if selections is None:
            return
        local_deferred_resolutions = DeferredResolutionSet() if deferred_resolutions is None else deferred_resolutions
        for resolver in self.get_resolvers():
            if resolver.is_selected(selections):
                resolver.resolve(self, selections, local_deferred_resolutions)
        if deferred_resolutions is None:
            local_deferred_resolutions.resolve()

    def unresolve_all(self):
        for resolver in self.get_resolvers():
            resolver.unresolve(self)

    def get_cache_header(self, exclude_resolvers: Optional[Set[str]] = None) -> CacheHeader:
        if exclude_resolvers is None:
            exclude_resolvers = set()
        cache_header = self.get_store().get_cache_header(self)
        cache_header = cache_header.combine_with(self._resolver_cache_headers(exclude_resolvers))
        return cache_header

    def _resolver_cache_headers(self, exclude_resolvers: Optional[Set[str]] = None) -> Iterator[CacheHeader]:
        for resolver in self.get_resolvers():
            if resolver.name not in exclude_resolvers and resolver.is_resolved(self):
                yield from resolver.get_cache_headers(self)

    @classmethod
    def get_meta(cls) -> PersistyMeta:
        store = cls.get_store()
        schemas = store.schemas
        return PersistyMeta(
            name=store.name,
            capabilities=store.capabilities,
            schema_for_create=cls._filter_schema(schemas.create, 'filter_create_schema'),
            schema_for_read=cls._filter_schema(schemas.read, 'filter_read_schema'),
            schema_for_update=cls._filter_schema(schemas.update, 'filter_update_schema'),
            schema_for_search=cls._filter_schema(schemas.search, 'filter_search_schema')
        )

    @classmethod
    def _filter_schema(cls, schema: SchemaABC[T], filter_name: str) -> SchemaABC:
        for resolver in cls.get_resolvers():
            schema = getattr(resolver, filter_name)(schema)
        return schema

    def __eq__(self, other):
        for f in dataclasses.fields(self._get_wrapped_class()):
            if getattr(self, f.name) != getattr(other, f.name, None):
                return False
        return True
