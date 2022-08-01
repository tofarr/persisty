from dataclasses import dataclass

from persisty.util import UNDEFINED


@dataclass
class EntityPropertyDescriptor:
    name: str

    def __get__(self, instance, owner):
        return instance.__local_values__.__dict__.get(self.name, UNDEFINED)

    def __set__(self, instance, value):
        instance.__local_values__.__dict__[self.name] = value
