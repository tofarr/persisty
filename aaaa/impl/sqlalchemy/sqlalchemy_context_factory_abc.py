from abc import ABC, abstractmethod
from typing import Optional

from marshy.factory.impl_marshaller_factory import get_impls

from aaaa.impl.sqlalchemy.sqlalchemy_context import SqlalchemyContext


class SqlalchemyContextFactoryABC(ABC):
    priority: int = 100

    @abstractmethod
    def create(self) -> Optional[SqlalchemyContext]:
        """Create a new context"""


def create_default_context() -> Optional[SqlalchemyContext]:
    factories = list(get_impls(SqlalchemyContextFactoryABC))
    factories.sort(key=lambda f: f.priority, reverse=True)
    for factory in factories:
        context = factory().create()
        if context:
            return context
