from typing import Optional, get_type_hints

from marshy import get_default_context
from marshy.marshaller.marshaller_abc import MarshallerABC
from schemey import Schema

from persisty.util.undefined import UNDEFINED


class EntityFieldDescriptor:
    __marshaller_context__ = get_default_context()
    name: Optional[str] = None
    schema: Schema = None
    marshaller: MarshallerABC = None

    def __set_name__(self, owner, name):
        self.name = name
        type_hints = get_type_hints(owner)
        type_ = type_hints.get(name)
        if not type_:
            raise ValueError("not_type_hint:{name}")
        self.type = type_
        self.marshaller = self.__marshaller_context__.get_marshaller(type_)

    def __get__(self, instance, owner):
        value = instance.__local_values__.get(self.name, UNDEFINED)
        if value is not UNDEFINED:
            value = self.marshaller.load(value)
        return value

    def __set__(self, instance, value):
        if value is UNDEFINED:
            instance.__local_values__.pop(self.name)
            return
        value = self.marshaller.dump(value)
        instance.__local_values__[self.name] = value
