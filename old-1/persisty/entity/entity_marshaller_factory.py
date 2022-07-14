import inspect
from typing import Type, Optional, TypeVar

from marshy.factory.marshaller_factory_abc import MarshallerFactoryABC
from marshy.marshaller.marshaller_abc import MarshallerABC
from marshy.marshaller_context import MarshallerContext

from persisty.entity.entity_abc import EntityABC
from persisty.entity.entity_marshaller import EntityMarshaller

T = TypeVar('T', bound=EntityABC)


class EntityMarshallerFactory(MarshallerFactoryABC):

    def create(self, context: MarshallerContext, type_: Type) -> Optional[MarshallerABC]:
        if inspect.isclass(type_) and issubclass(type_, EntityABC):
            return EntityMarshaller(type_, context)
