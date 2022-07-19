from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TypeVar, Generic
from uuid import UUID, uuid4

from persisty.key_config.key_config_abc import KeyConfigABC

T = TypeVar('T')


class BatchEditABC(ABC):
    @abstractmethod
    @property
    def id(self) -> UUID:
        """ Get a unique id for this edit"""

    @abstractmethod
    def get_key(self, key_config: KeyConfigABC) -> str:
        """ Get the key for the stored being edited """


@dataclass
class Create(BatchEditABC, Generic[T]):
    item: T = None
    id: UUID = field(default_factory=uuid4)

    def get_key(self, key_config: KeyConfigABC) -> str:
        return key_config.get_key(self.item)


@dataclass
class Update(BatchEditABC, Generic[T]):
    updates: T = None
    id: UUID = field(default_factory=uuid4)

    def get_key(self, key_config: KeyConfigABC) -> str:
        return key_config.get_key(self.updates)


@dataclass
class Delete(BatchEditABC):
    key: str
    id: UUID = field(default_factory=uuid4)

    def get_key(self, key_config: KeyConfigABC) -> str:
        return self.key
