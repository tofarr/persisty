import importlib
from abc import ABC
from dataclasses import dataclass
from typing import Union, Type, TypeVar

from persisty.obj_graph.entity_abc import EntityABC
from persisty.obj_graph.resolver.resolver_abc import ResolverABC

T = TypeVar('T', bound=EntityABC)


@dataclass(frozen=True)
class EntityResolverABC(ResolverABC[T], ABC):
    entity_type: Union[str, Type[EntityABC]]

    def get_entity_type(self) -> Type[T]:
        if isinstance(self.entity_type, str):
            import_path = self.entity_type.split('.')
            import_module = '.'.join(import_path[:-1])
            imported_module = importlib.import_module(import_module)
            entity_type = getattr(imported_module, import_path[-1])
            object.__setattr__(self, 'entity_type', entity_type)
        return self.entity_type
