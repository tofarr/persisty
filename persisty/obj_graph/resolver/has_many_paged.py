from typing import Optional, Callable, TypeVar, Union, Type

from persisty.item_filter import AttrFilter, AttrFilterOp
from persisty.obj_graph.deferred.deferred_resolution_set import DeferredResolutionSet
from persisty.obj_graph.entity_abc import EntityABC
from persisty.obj_graph.resolver.before_destroy import OnDestroy
from persisty.obj_graph.resolver.has_many import get_entity_type
from persisty.obj_graph.resolver.resolver_abc import ResolverABC, A
from persisty.obj_graph.selection_set import SelectionSet
from persisty.page import Page
from persisty.search_filter import SearchFilter

B = TypeVar('B')


class HasManyPaged(ResolverABC[A, B]):

    def __init__(self,
                 foreign_key_attr: str,
                 entity_type: Union[str, EntityABC[B]],
                 inverse_attr: Optional[str] = None,
                 limit: int = 20,
                 on_destroy: OnDestroy = OnDestroy.NO_ACTION,
                 private_name_: Optional[str] = None,
                 resolved_type: Optional[Type[B]] = None):
        super().__init__(private_name_, resolved_type)
        self.foreign_key_attr = foreign_key_attr
        self.entity_type = entity_type
        self.inverse_attr = inverse_attr
        self.limit = limit
        self.on_destroy = on_destroy
        self._entity_type = None

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
        self._entity_type = entity_type = get_entity_type(self.resolved_type, Page)
        return entity_type

    def _search(self, owner_instance: A):
        key = owner_instance.get_key()
        if key is None:
            return
        search_filter = SearchFilter(AttrFilter(self.foreign_key_attr, AttrFilterOp.eq, key))
        entities = self._entity_type().paged_search(search_filter, limit=self.limit)
        return entities

    def before_destroy(self, owner_instance: A):
        if self.on_destroy == OnDestroy.CASCADE:
            for entity in self._search(owner_instance):
                entity.destroy()
        elif self.on_destroy == OnDestroy.NULLIFY:
            for entity in self._search(owner_instance):
                setattr(entity, self.foreign_key_attr, None)
                entity.update()
