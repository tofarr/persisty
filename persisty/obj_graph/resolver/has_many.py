from collections import abc
from typing import Optional, Callable, TypeVar, Type, Iterable, Iterator

import typing_inspect
from marshy.utils import resolve_forward_refs

from persisty.cache_header import CacheHeader
from persisty.errors import PersistyError
from persisty2.item_filter import AttrFilter, AttrFilterOp
from persisty.obj_graph.deferred.deferred_resolution_set import DeferredResolutionSet
from persisty.obj_graph.resolver.before_destroy import OnDestroy
from persisty.obj_graph.resolver.resolver_abc import ResolverABC, A
from persisty.obj_graph.selection_set import SelectionSet
from schemey.object_schema import ObjectSchema
from schemey.schema_abc import SchemaABC
from persisty2.search_filter import SearchFilter

B = TypeVar('B')


class HasMany(ResolverABC[A, B]):
    """
    Resolver that implies owner object is connected to a small number of other
    objects. (If the number is likely to be large, use HasManyPaged instead -
    it returns paged results and does not have the same assumptions around updates)
    """

    def __init__(self,
                 foreign_key_attr: str,
                 on_destroy: OnDestroy = OnDestroy.NO_ACTION,
                 private_name_: Optional[str] = None,
                 _is_overridden_name: Optional[str] = None,
                 resolved_type: Optional[Type[B]] = None):
        super().__init__(private_name_, resolved_type)
        self.foreign_key_attr = foreign_key_attr
        self.on_destroy = on_destroy
        self._entity_type = None  # Resolve later
        self.is_overridden_name = None

    def __set_name__(self, owner, name):
        super().__set_name__(owner, name)
        if self.is_overridden_name is None:
            self.is_overridden_name = f'is_{name}_overridden'

    def __set__(self, owner_instance: A, value: B):
        super().__set__(owner_instance, value)
        setattr(owner_instance, self.is_overridden_name, True)

    def resolve_value(self,
                      owner_instance: A,
                      callback: Callable[[Optional[Iterable[B]]], None],
                      sub_selections: Optional[SelectionSet],
                      deferred_resolutions: Optional[DeferredResolutionSet] = None):
        entities = self._search(owner_instance)
        if entities is None:
            setattr(owner_instance, self.is_overridden_name, False)
            callback(None)
            return
        entities = tuple(entities)
        if sub_selections:
            for entity in entities:
                entity.resolve_all(sub_selections, deferred_resolutions)
        setattr(owner_instance, self.is_overridden_name, False)
        callback(entities)

    def unresolve(self, owner_instance: A):
        super().unresolve(owner_instance)
        setattr(owner_instance, self.is_overridden_name, False)

    def _get_entity_type(self):
        entity_type = self._entity_type
        if entity_type:
            return entity_type
        self._entity_type = get_entity_type(self.resolved_type, (Iterable, abc.Iterable))
        return self._entity_type

    def _search(self, owner_instance: A):
        key = owner_instance.get_key()
        if key is None:
            return
        search_filter = SearchFilter(AttrFilter(self.foreign_key_attr, AttrFilterOp.eq, key))
        entities = self._get_entity_type().search(search_filter)
        return entities

    def is_overridden(self, owner_instance: A) -> bool:
        return getattr(owner_instance, self.is_overridden_name, False)

    def before_destroy(self, owner_instance: A):
        setattr(owner_instance, self.is_overridden_name, False)
        if self.on_destroy == OnDestroy.CASCADE:
            for entity in self._search(owner_instance):
                entity.destroy()
        elif self.on_destroy == OnDestroy.NULLIFY:
            for entity in self._search(owner_instance):
                setattr(entity, self.foreign_key_attr, None)
                entity.update()

    def after_create(self, owner_instance: A):
        if self.is_overridden(owner_instance):
            setattr(owner_instance, self.is_overridden_name, False)
            key = owner_instance.get_key()
            for entity in getattr(owner_instance, self.private_name):
                setattr(entity, self.foreign_key_attr, key)
                entity.save()

    def after_update(self, owner_instance: A):
        if self.is_overridden(owner_instance):
            setattr(owner_instance, self.is_overridden_name, False)
            existing_by_key = {e.get_key(): e for e in self._search(owner_instance)}
            key = owner_instance.get_key()
            entities = getattr(owner_instance, self.private_name) or []
            for e in entities:
                setattr(e, self.foreign_key_attr, key)
                e.save()
                foreign_key = e.get_key()
                existing_by_key.pop(foreign_key, None)
            for e in existing_by_key.values():
                e.destroy()

    def get_cache_headers(self, owner_instance: A, selections: SelectionSet) -> Iterator[CacheHeader]:
        entities = getattr(owner_instance, self.name)
        yield from (entity.get_cache_header(selections) for entity in entities)

    def filter_read_schema(self, schema: SchemaABC) -> SchemaABC:
        if not isinstance(schema, ObjectSchema):
            return schema
        property_schemas = list(schema.property_schemas)
        has_many_schema = optional_schema(HasManySchema(self.key_attr, self._entity_type))
        property_schemas.append(has_many_schema)
        return ObjectSchema(tuple(property_schemas))

    def filter_create_schema(self, schema: SchemaABC) -> SchemaABC:
        return self.filter_read_schema(schema)

    def filter_update_schema(self, schema: SchemaABC) -> SchemaABC:
        return self.filter_read_schema(schema)

    def filter_search_schema(self, schema: SchemaABC) -> SchemaABC:
        return self.filter_read_schema(schema)


def get_entity_type(from_type, expected: Iterable[Type]):
    resolved_type = resolve_forward_refs(from_type)
    origin = typing_inspect.get_origin(resolved_type)
    if origin not in expected:
        raise PersistyError(f'invalid_type:{from_type}:{expected}')
    entity_type = typing_inspect.get_args(resolved_type)[0]
    return entity_type
