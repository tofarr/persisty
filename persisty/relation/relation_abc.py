from abc import ABC, abstractmethod

from marshy.types import ExternalItemType, ExternalType

from persisty.persisty_context import PersistyContext


class RelationABC(ABC):
    @abstractmethod
    def get_name(self):
        """Get the name for this relation"""

    @abstractmethod
    def resolve_for(
        self, item: ExternalItemType, context: PersistyContext
    ) -> ExternalType:
        """Resolve this relation for the item given"""
