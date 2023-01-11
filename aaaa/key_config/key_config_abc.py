from abc import ABC, abstractmethod
from typing import Optional, TypeVar, Generic

T = TypeVar("T")


class KeyConfigABC(ABC, Generic[T]):
    """General object for sorting items"""

    @abstractmethod
    def to_key_str(self, item: T) -> str:
        """Get the key from the stored given."""

    @abstractmethod
    def from_key_str(self, key: Optional[str], target: T):
        """Set the key for the stored given"""

    @abstractmethod
    def is_required_attr(self, attr_name: str) -> bool:
        """Determine whether the field given is required to be non null by this key config"""
