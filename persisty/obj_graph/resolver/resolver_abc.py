from abc import ABC, abstractmethod
from typing import TypeVar, Generic, Optional

from persisty.obj_graph.deferred.deferred_resolution_set import DeferredResolutionSet
from persisty.obj_graph.selection_set import SelectionSet

T = TypeVar('T')


class ResolverABC(ABC, Generic[T]):

    @abstractmethod
    def resolve(self,
                owner: T,
                attr_name: str,
                sub_selections: Optional[SelectionSet],
                deferred_resolutions: DeferredResolutionSet):
        """ Resolve for the owner given """
