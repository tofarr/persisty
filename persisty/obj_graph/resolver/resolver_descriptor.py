from typing import Type, Generic, TypeVar, Optional

from persisty.obj_graph.deferred.deferred_resolution_set import DeferredResolutionSet
from persisty.obj_graph.resolver.resolver_abc import ResolverABC
from persisty.obj_graph.selection_set import SelectionSet

A = TypeVar('A')
B = TypeVar('B')


class _NotInitializedType:
    pass


NOT_INITIALIZED = _NotInitializedType()


class ResolverDescriptor(Generic[A, B]):
    """
    Python Field Descriptor implementing lazy resolution logic.
    """

    def __init__(self, resolver: ResolverABC[A], private_name_: Optional[str] = None):
        self.name = None
        self.resolver = resolver
        self.private_name = private_name_

    def is_resolved(self, owner_instance: A) -> bool:
        value = getattr(owner_instance, self.private_name, NOT_INITIALIZED)
        return value is not NOT_INITIALIZED

    def is_selected(self, selections: Optional[SelectionSet]) -> bool:
        is_selected = bool(selections and selections.get_selections(self.name))
        return is_selected

    def resolve(self,
                owner_instance: A,
                selections: Optional[SelectionSet] = None,
                deferred_resolutions: Optional[DeferredResolutionSet] = None):
        sub_selections = selections.get_selections(self.name) if selections else None
        if not sub_selections:
            return
        local_deferred_resolutions = deferred_resolutions or DeferredResolutionSet()
        self.resolver.resolve(owner_instance, self.private_name, sub_selections, local_deferred_resolutions)
        if deferred_resolutions is None:
            local_deferred_resolutions.resolve()

    def __set_name__(self, owner, name):
        self.name = name
        self.private_name = f'_{name}'

    def __get__(self, owner_instance: A, owner_type: Type[A]) -> B:
        value = getattr(owner_instance, self.private_name, NOT_INITIALIZED)
        if value is NOT_INITIALIZED:
            deferred_resolutions = DeferredResolutionSet()
            self.resolver.resolve(owner_instance, self.private_name, None, deferred_resolutions)
            deferred_resolutions.resolve()
        value = getattr(owner_instance, self.private_name)
        return value

    def __set__(self, owner_instance: A, value: B):
        setattr(owner_instance, self.private_name, value)


def private_name(name: str) -> str:
    return f'_{name}'
