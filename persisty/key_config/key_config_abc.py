from abc import ABC, abstractmethod
from typing import Optional, TypeVar, Generic, Iterator, FrozenSet

from marshy.types import ExternalItemType

T = TypeVar("T")


class KeyConfigABC(ABC, Generic[T]):
    """ Object for extracting / inserting string keys into stored items. """

    @abstractmethod
    def to_key_str(self, item: T) -> str:
        """Get the key from the stored given."""

    @abstractmethod
    def from_key_str(self, key: Optional[str], target: T):
        """Set the key for the stored given"""

    @abstractmethod
    def to_key_dict(self, key: Optional[str]) -> ExternalItemType:
        """Set the key for the stored given"""

    @abstractmethod
    def get_key_attrs(self) -> FrozenSet[str]:
        """Get the required attributes for this key"""
