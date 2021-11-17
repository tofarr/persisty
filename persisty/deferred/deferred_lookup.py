from abc import ABC
from dataclasses import dataclass, field
from typing import TypeVar, Generic, Dict, List, Callable, Type

from persisty.deferred.deferred_resolution_abc import DeferredResolutionABC
from persisty.obj_graph.old_entity_abc import EntityABC

T = TypeVar('T', bound=EntityABC)


@dataclass(frozen=True)
class DeferredLookup(DeferredResolutionABC, ABC, Generic[T]):
    entity_type: Type[T]
    resolved: Dict[str, T] = field(default_factory=dict)
    to_resolve: Dict[str, List[Callable[[T], None]]] = field(default_factory=dict)

    def resolve(self) -> bool:
        if not self.to_resolve:
            return False
        entities = self.entity_type.read_all(iter(self.to_resolve.keys()))
        for entity in entities:
            key = entity.get_key()
            self.resolved[key] = entity
            callbacks = self.to_resolve[key]
            for callback in callbacks:
                callback(entity)
        self.to_resolve.clear()
        return True
