from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TypeVar, Generic
from uuid import UUID, uuid4

from persisty.key_config.key_config_abc import KeyConfigABC

T = TypeVar("T")


@dataclass
class BatchEditABC(ABC):
    @abstractmethod
    def get_key(self, key_config: KeyConfigABC) -> str:
        """Get the key for the stored being edited"""

    @abstractmethod
    def get_id(self) -> UUID:
        """Get the unique identifier for the edit"""


@dataclass
class Create(BatchEditABC, Generic[T]):
    item: T = None
    id: UUID = field(default_factory=uuid4)

    def get_key(self, key_config: KeyConfigABC) -> str:
        return key_config.to_key_str(self.item)

    def get_id(self) -> UUID:
        return self.id


@dataclass
class Update(BatchEditABC, Generic[T]):
    updates: T = None
    id: UUID = field(default_factory=uuid4)

    def get_key(self, key_config: KeyConfigABC) -> str:
        return key_config.to_key_str(self.updates)

    def get_id(self) -> UUID:
        return self.id


@dataclass
class Delete(BatchEditABC):
    key: str
    id: UUID = field(default_factory=uuid4)

    def get_key(self, key_config: KeyConfigABC) -> str:
        return self.key

    def get_id(self) -> UUID:
        return self.id
