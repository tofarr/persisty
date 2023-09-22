from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterator, TypeVar, Generic

from servey.action.action import Action

ROUTE = "starlette.routing.Route"
_StoreABC = "persisty.store.store_abc.StoreABC"
T = TypeVar("T")


class ActionFactoryABC(Generic[T], ABC):

    @abstractmethod
    def create_actions(self, store: _StoreABC) -> Iterator[Action]:
        """Create actions for this store"""

    @abstractmethod
    def create_routes(self, store: _StoreABC) -> Iterator[ROUTE]:
        """Create routes for this store"""
