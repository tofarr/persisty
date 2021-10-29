from abc import ABC, abstractmethod
from typing import TypeVar, Generic, Optional, Type, Callable

from persisty.errors import PersistyError
from persisty.obj_graph.deferred.deferred_resolution_set import DeferredResolutionSet
from persisty.obj_graph.selection_set import SelectionSet, from_list


class _NotInitializedType:
    pass


NOT_INITIALIZED = _NotInitializedType()
A = TypeVar('A')
B = TypeVar('B')


class ResolverABC(ABC, Generic[A, B]):

    def __init__(self, private_name_: Optional[str] = None, resolved_type: Optional[Type[B]] = None):
        self.name = None
        self.private_name = private_name_
        self.resolved_type = resolved_type

    def __set_name__(self, owner, name):
        self.name = name
        if self.private_name is None:
            self.private_name = f'_{name}'
        if self.resolved_type is None:
            self.resolved_type = owner.__annotations__.get(self.name)
            if not self.resolved_type:
                raise ValueError(f'missing_annotation:{owner}:{self.name}')

    def __get__(self, owner_instance: A, owner_type: Type[A]) -> B:
        if not self.is_resolved(owner_instance):
            selections = from_list([self.name])
            self.resolve(owner_instance, selections)
        value = getattr(owner_instance, self.private_name)
        return value

    def __set__(self, owner_instance: A, value: B):
        setattr(owner_instance, self.private_name, value)

    def reset(self, owner_instance: A):
        setattr(owner_instance, self.private_name, NOT_INITIALIZED)

    def is_resolved(self, owner_instance: A) -> bool:
        value = getattr(owner_instance, self.private_name, NOT_INITIALIZED)
        is_resolved = value is not NOT_INITIALIZED
        return is_resolved

    def is_selected(self, selections: SelectionSet):
        return bool(selections.get_selections(self.name))

    def resolve(self,
                owner_instance: A,
                selections: Optional[SelectionSet] = None,
                deferred_resolutions: Optional[DeferredResolutionSet] = None):
        """ Resolve this resolver for the owner given """
        local_deferred_resolutions = DeferredResolutionSet() if deferred_resolutions is None else deferred_resolutions
        sub_selections = selections.get_selections(self.name) if selections else None

        def callback(value: Optional[B]):
            setattr(owner_instance, self.private_name, value)

        self.resolve_value(owner_instance, callback, sub_selections, local_deferred_resolutions)
        if not deferred_resolutions:
            local_deferred_resolutions.resolve()

    @abstractmethod
    def resolve_value(self, owner_instance: A,
                      callback: Callable[[B], None],
                      sub_selections: Optional[SelectionSet],
                      deferred_resolutions: Optional[DeferredResolutionSet] = None):
        """ Resolve a value and pass it to the callback"""

    def before_create(self, owner_instance: A):
        """ Hook invoked immediately before an entity is created """
        pass

    def after_create(self, owner_instance: A):
        """ Hook invoked immediately before an entity is created """
        pass

    def before_update(self, owner_instance: A):
        """ Hook invoked immediately before an entity is updated """
        pass

    def after_update(self, owner_instance: A):
        """ Hook invoked immediately after an entity is updated """
        pass

    def before_destroy(self, owner_instance: A):
        """ Hook invoked immediately before an entity is destroyed """
        pass

    def after_destroy(self, owner_instance: A):
        """ Hook invoked immediately after an entity is destroyed """
        pass
