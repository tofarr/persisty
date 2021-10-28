import importlib
from typing import Optional, Callable, TypeVar, Union, Type

from persisty.item_filter import AttrFilterOp, AttrFilter
from persisty.obj_graph.deferred.deferred_resolution_set import DeferredResolutionSet
from persisty.obj_graph.entity_abc import EntityABC
from persisty.obj_graph.resolver.before_destroy import OnDestroy
from persisty.obj_graph.resolver.resolver_abc import ResolverABC, A
from persisty.obj_graph.selection_set import SelectionSet
from persisty.search_filter import SearchFilter

B = TypeVar('B')


class Count(ResolverABC[A, int]):

    def __init__(self,
                 foreign_key_attr: str,
                 entity_type: Union[str, EntityABC[B]],
                 inverse_attr: Optional[str] = None,
                 on_destroy: OnDestroy = OnDestroy.NO_ACTION,
                 private_name_: Optional[str] = None,
                 resolved_type: Optional[Type[B]] = None):
        super().__init__(private_name_, resolved_type)
        self.foreign_key_attr = foreign_key_attr
        self.entity_type = entity_type
        self.inverse_attr = inverse_attr
        self.on_destroy = on_destroy

    def resolve_value(self,
                      owner_instance: A,
                      callback: Callable[[int], None],
                      sub_selections: Optional[SelectionSet],
                      deferred_resolutions: Optional[DeferredResolutionSet] = None):
        key = owner_instance.get_key()
        if key is None:
            callback(0)
            return
        search_filter = SearchFilter(AttrFilter(self.foreign_key_attr, AttrFilterOp.eq, key))
        count = self._entity_type().count(search_filter)
        callback(count)

    def _entity_type(self):
        if isinstance(self.entity_type, str):
            self.entity_type = type_from_name(self.entity_type)
        return self.entity_type


def type_from_name(entity_type: str):
    import_path = entity_type.split('.')
    import_module = '.'.join(import_path[:-1])
    imported_module = importlib.import_module(import_module)
    entity_type = getattr(imported_module, import_path[-1])
    return entity_type
