from typing import Optional, Callable, TypeVar, Type

import typing_inspect
from marshy.utils import resolve_forward_refs

from persisty.errors import PersistyError
from persisty.item_filter import AttrFilter, AttrFilterOp
from persisty.obj_graph.deferred.deferred_resolution_set import DeferredResolutionSet
from persisty.obj_graph.resolver.before_destroy import OnDestroy
from persisty.obj_graph.resolver.resolver_abc import ResolverABC, A
from persisty.obj_graph.selection_set import SelectionSet
from persisty.search_filter import SearchFilter

B = TypeVar('B')


class HasMany(ResolverABC[A, B]):

    def __init__(self,
                 foreign_key_attr: str,
                 inverse_attr: Optional[str] = None,
                 on_destroy: OnDestroy = OnDestroy.NO_ACTION,
                 private_name_: Optional[str] = None,
                 resolved_type: Optional[Type[B]] = None):
        super().__init__(private_name_, resolved_type)
        self.foreign_key_attr = foreign_key_attr
        self.inverse_attr = inverse_attr
        self.on_destroy = on_destroy
        self._entity_type = None  # Resolve later

    def resolve_value(self,
                      owner_instance: A,
                      callback: Callable[[B], None],
                      sub_selections: Optional[SelectionSet],
                      deferred_resolutions: Optional[DeferredResolutionSet] = None):
        entities = list(self._search(owner_instance))
        if sub_selections:
            for entity in entities:
                entity.resolve_all(sub_selections, deferred_resolutions)
        if self.inverse_attr:
            for entity in entities:
                setattr(entity, self.inverse_attr, owner_instance)
        callback(entities)

    def _get_entity_type(self):
        entity_type = self._entity_type
        if entity_type:
            return entity_type
        self._entity_type = get_entity_type(self.resolved_type, list)
        return self._entity_type

    def _search(self, owner_instance: A):
        key = owner_instance.get_key()
        if key is None:
            return
        search_filter = SearchFilter(AttrFilter(self.foreign_key_attr, AttrFilterOp.eq, key))
        entities = self._get_entity_type().search(search_filter)
        return entities

    def before_destroy(self, owner_instance: A):
        if self.on_destroy == OnDestroy.CASCADE:
            for entity in self._search(owner_instance):
                entity.destroy()
        elif self.on_destroy == OnDestroy.NULLIFY:
            for entity in self._search(owner_instance):
                setattr(entity, self.foreign_key_attr, None)
                entity.update()


def get_entity_type(from_type, expected):
    resolved_type = resolve_forward_refs(from_type)
    origin = typing_inspect.get_origin(resolved_type)
    if origin is not expected:
        raise PersistyError(f'invalid_type:{from_type}:{expected}')
    entity_type = typing_inspect.get_args(resolved_type)[0]
    return entity_type
