from typing import Optional, Callable, TypeVar, Type, Iterator

from persisty.cache_header import CacheHeader
from persisty.errors import PersistyError
from persisty.item_filter import AttrFilter, AttrFilterOp
from persisty.obj_graph.deferred.deferred_resolution_set import DeferredResolutionSet
from persisty.obj_graph.resolver.before_destroy import OnDestroy
from persisty.obj_graph.resolver.has_many import get_entity_type
from persisty.obj_graph.resolver.resolver_abc import ResolverABC, A
from persisty.obj_graph.selection_set import SelectionSet
from persisty.page import Page
from schemey.object_schema import ObjectSchema
from schemey.schema_abc import SchemaABC
from persisty.search_filter import SearchFilter
from persisty.util import secure_hash

B = TypeVar('B')


class HasManyPaged(ResolverABC[A, B]):

    def __init__(self,
                 foreign_key_attr: str,
                 inverse_attr: Optional[str] = None,
                 limit: int = 20,
                 on_destroy: OnDestroy = OnDestroy.NO_ACTION,
                 private_name_: Optional[str] = None,
                 resolved_type: Optional[Type[B]] = None):
        super().__init__(private_name_, resolved_type)
        self.foreign_key_attr = foreign_key_attr
        self.inverse_attr = inverse_attr
        self.limit = limit
        self.on_destroy = on_destroy
        self._entity_type = None

    def resolve_value(self,
                      owner_instance: A,
                      callback: Callable[[Optional[Page[B]]], None],
                      sub_selections: Optional[SelectionSet],
                      deferred_resolutions: Optional[DeferredResolutionSet] = None):
        search_filter = self._search_filter(owner_instance)
        if search_filter is None:
            callback(None)
            return
        page = self._get_entity_type().paged_search(search_filter, limit=self.limit)
        if sub_selections:
            for entity in page.items:
                entity.resolve_all(sub_selections, deferred_resolutions)
        if self.inverse_attr:
            for entity in page.items:
                setattr(entity, self.inverse_attr, owner_instance)
        callback(page)

    def _get_entity_type(self):
        entity_type = self._entity_type
        if entity_type:
            return entity_type
        self._entity_type = get_entity_type(self.resolved_type, (Page,))
        return self._entity_type

    def _search_filter(self, owner_instance: A):
        key = owner_instance.get_key()
        if key is None:
            return
        search_filter = SearchFilter(AttrFilter(self.foreign_key_attr, AttrFilterOp.eq, key))
        return search_filter

    def _search(self, owner_instance: A):
        search_filter = self._search_filter(owner_instance)
        entities = self._get_entity_type().search(search_filter)
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

    def get_cache_headers(self, owner_instance: A) -> Iterator[CacheHeader]:
        # last modified of paged is unknowable (Suppose something on another page changes?)
        entities = getattr(owner_instance, self.name)
        exclude_resolvers = [self.inverse_attr] if self.inverse_attr else []
        yield from (e.get_cache_header(exclude_resolvers) for e in entities)

    def filter_read_schema(self, schema: SchemaABC) -> SchemaABC:
        if not isinstance(schema, ObjectSchema):
            return schema
        property_schemas = list(schema.property_schemas)
        has_many_schema = optional_schema(HasManyPagedSchema(self.key_attr, self._entity_type))
        property_schemas.append(has_many_schema)
        return ObjectSchema(tuple(property_schemas))

    def filter_search_schema(self, schema: SchemaABC) -> SchemaABC:
        return self.filter_read_schema(schema)

