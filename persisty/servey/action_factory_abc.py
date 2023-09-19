from abc import ABC
from dataclasses import dataclass
from typing import Iterator, TypeVar, Generic

from servey.action.action import Action

ROUTE = "starlette.routing.Route"
_StoreABC = "persisty.store.store_abc.StoreABC"
T = TypeVar("T")


@dataclass
class ActionFactoryABC(Generic[T], ABC):
    def create_actions(self, store: _StoreABC) -> Iterator[Action]:
        """Create actions for this factory"""

    def create_routes(self, store: _StoreABC) -> Iterator[ROUTE]:
        """Create routes for this factory"""
