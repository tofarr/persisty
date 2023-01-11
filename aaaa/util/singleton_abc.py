from abc import ABC

from marshy import ExternalType
from marshy.marshaller.marshaller_abc import MarshallerABC, T


class SingletonABC(ABC):
    """Abstract singleton implementation"""

    def __new__(cls):
        instance = getattr(cls, "__instance", None)
        if instance is None:
            instance = object.__new__(cls)
            setattr(cls, "__instance", instance)
        return instance

    def __repr__(self):
        return self.__class__.__name__

    # noinspection PyUnusedLocal
    @classmethod
    def __marshaller_factory__(cls, marshaller_context):
        return SingletonMarshaller(cls)


class SingletonMarshaller(MarshallerABC[T]):
    def load(self, item: ExternalType) -> T:
        return self.marshalled_type()

    def dump(self, item: T) -> ExternalType:
        return self.marshalled_type.__name__
