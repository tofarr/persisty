from abc import ABC, abstractmethod
from typing import Iterator, TypeVar, Generic

from servey.action.action import Action

_StoreMeta = "persisty.store.store_meta.StoreMeta"


class ActionFactoryABC(ABC):
    @abstractmethod
    def create_actions(self, store_meta: _StoreMeta) -> Iterator[Action]:
        """Create actions for the store given"""
