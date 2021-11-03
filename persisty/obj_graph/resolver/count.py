from typing import Optional, Callable, Type

from marshy.factory.optional_marshaller_factory import get_optional_type
from marshy.utils import resolve_forward_refs

from persisty.errors import PersistyError
from persisty.item_filter import AttrFilterOp, AttrFilter
from persisty.obj_graph.deferred.deferred_resolution_set import DeferredResolutionSet
from persisty.obj_graph.entity_abc import EntityABC
from persisty.obj_graph.resolver.before_destroy import OnDestroy
from persisty.obj_graph.resolver.resolver_abc import ResolverABC, A
from persisty.obj_graph.selection_set import SelectionSet
from persisty.search_filter import SearchFilter


class Count(ResolverABC[A, int]):

    def __init__(self,
                 foreign_key_attr: str,
                 entity_type: Type[EntityABC],
                 on_destroy: OnDestroy = OnDestroy.NO_ACTION,
                 private_name_: Optional[str] = None):
        super().__init__(private_name_, int)
        self.foreign_key_attr = foreign_key_attr
        entity_type = resolve_forward_refs(entity_type)
        entity_type = get_optional_type(entity_type) or entity_type
        # noinspection PyTypeChecker
        self._entity_type: Type[EntityABC] = entity_type
        self.on_destroy = on_destroy

    def resolve_value(self,
                      owner_instance: A,
                      callback: Callable[[Optional[int]], None],
                      sub_selections: Optional[SelectionSet],
                      deferred_resolutions: Optional[DeferredResolutionSet] = None):
        key = owner_instance.get_key()
        if key is None:
            callback(None)
            return
        item_filter = AttrFilter(self.foreign_key_attr, AttrFilterOp.eq, key)
        count = self._entity_type.count(item_filter)
        callback(count)

    def __set__(self, instance, value):
        raise PersistyError(f'set_not_supported:{self.name}')

    def before_destroy(self, owner_instance: A):
        if self.on_destroy == OnDestroy.NO_ACTION or owner_instance.get_key() is None:
            return
        search_filter = SearchFilter(AttrFilter(self.foreign_key_attr, AttrFilterOp.eq, owner_instance.get_key()))
        entities = self._entity_type.search(search_filter)
        if self.on_destroy == OnDestroy.CASCADE:
            for entity in entities:
                entity.destroy()
        elif self.on_destroy == OnDestroy.NULLIFY:
            for entity in entities:
                setattr(entity, self.foreign_key_attr, None)
                entity.update()
