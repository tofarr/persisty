from abc import ABC, abstractmethod
from typing import Optional, TypeVar, Generic

T = TypeVar("T")


class ObjKeyConfigABC(ABC, Generic[T]):
    """General object for sorting items"""

    @abstractmethod
    def from_key_str(self, item: T) -> str:
        """Get the key from the stored given."""

    @abstractmethod
    def to_key_str(self, key: Optional[str], item: T):
        """Set the key for the stored given"""
