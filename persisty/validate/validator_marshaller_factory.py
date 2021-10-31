from typing import Type, Optional

from marshy.factory.marshaller_factory_abc import MarshallerFactoryABC
from marshy.marshaller.marshaller_abc import MarshallerABC
from marshy.marshaller_context import MarshallerContext

from persisty.validate.validator_abc import ValidatorABC
from persisty.validate.validator_marshaller import ValidatorMarshaller


class ValidatorMarshallerFactory(MarshallerFactoryABC):

    def create(self, context: MarshallerContext, type_: Type) -> Optional[MarshallerABC]:
        if isinstance(type_, ValidatorABC):
            return ValidatorMarshaller()
