from typing import Optional, Callable, TypeVar, Type, Iterator

from persisty.cache_header import CacheHeader
from old.persisty import PersistyError
from persisty.item_filter import AttrFilter, AttrFilterOp
from persisty.deferred.deferred_resolution_set import DeferredResolutionSet
from persisty.obj_graph.resolver.before_destroy import OnDestroy
from persisty.obj_graph.resolver.has_many import get_entity_type
from persisty.obj_graph.resolver.resolver_abc import ResolverABC, A
from persisty.obj_graph.selection_set import SelectionSet
from persisty.page import Page
from schemey.object_schema import ObjectSchema
from schemey.schema_abc import SchemaABC
from old.persisty2.storage_filter import StorageFilter

B = TypeVar('B')


class HasManyPaged(ResolverABC[A, B]):

    def __init__(self,
                 foreign_key_attr: str,
                 limit: int = 20,
                 on_destroy: OnDestroy = OnDestroy.NO_ACTION,
                 private_name_: Optional[str] = None,
                 resolved_type: Optional[Type[B]] = None):
        super().__init__(private_name_, resolved_type)
        self.foreign_key_attr = foreign_key_attr
        self.limit = limit
        self.on_destroy = on_destroy
        self._entity_type = None

    def resolve_value(self,
                      owner_instance: A,
                      callback: Callable[[Optional[Page[B]]], None],
                      sub_selections: Optional[SelectionSet],
                      deferred_resolutions: Optional[DeferredResolutionSet] = None):
        storage_filter = self._storage_filter(owner_instance)
        if storage_filter is None:
            callback(None)
            return
        page = self._get_entity_type().paged_search(storage_filter, limit=self.limit)
        if sub_selections:
            for entity in page.items:
                entity.resolve_all(sub_selections, deferred_resolutions)
        callback(page)

    def _get_entity_type(self):
        entity_type = self._entity_type
        if entity_type:
            return entity_type
        self._entity_type = get_entity_type(self.resolved_type, (Page,))
        return self._entity_type

    def _storage_filter(self, owner_instance: A):
        key = owner_instance.get_key()
        if key is None:
            return
        storage_filter = StorageFilter(AttrFilter(self.foreign_key_attr, AttrFilterOp.eq, key))
        return storage_filter

    def _search(self, owner_instance: A):
        storage_filter = self._storage_filter(owner_instance)
        entities = self._get_entity_type().search(storage_filter)
        return entities

    def __set__(self, instance, value):
        raise PersistyError(f'set_not_supported:{self.name}')

    def before_destroy(self, owner_instance: A):
        if self.on_destroy == OnDestroy.CASCADE:
            for entity in self._search(owner_instance):
                entity.destroy()
        elif self.on_destroy == OnDestroy.NULLIFY:
            for entity in self._search(owner_instance):
                setattr(entity, self.foreign_key_attr, None)
                entity.update()

    def get_cache_headers(self, owner_instance: A, selections: SelectionSet) -> Iterator[CacheHeader]:
        # last modified of paged is unknowable (Suppose something on another page changes?)
        sub_selections = selections.get_selections('items')
        if sub_selections:
            page = getattr(owner_instance, self.name)
            yield from (e.get_cache_header(sub_selections) for e in page.items)

    def filter_read_schema(self, schema: SchemaABC) -> SchemaABC:
        if not isinstance(schema, ObjectSchema):
            return schema
        property_schemas = list(schema.property_schemas)
        has_many_schema = optional_schema(HasManyPagedSchema(self.key_attr, self._entity_type))
        property_schemas.append(has_many_schema)
        return ObjectSchema(tuple(property_schemas))

    def filter_search_schema(self, schema: SchemaABC) -> SchemaABC:
        return self.filter_read_schema(schema)

