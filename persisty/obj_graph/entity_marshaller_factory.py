from typing import Type, Optional

from marshy.factory.marshaller_factory_abc import MarshallerFactoryABC
from marshy.marshaller.marshaller_abc import MarshallerABC
from marshy.marshaller_context import MarshallerContext

from persisty.obj_graph.entity_abc import EntityABC


class EntityMarshallerFactory(MarshallerFactoryABC):

    def create(self, context: MarshallerContext, type_: Type) -> Optional[MarshallerABC]:
        if issubclass(type_, EntityABC):
            # Gather the resolver descriptors. Do not resolve any values as part of serialization
            raise ValueError()
        return None