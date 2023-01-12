from abc import ABC, abstractmethod
from typing import Optional, TypeVar, Generic, Iterator

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
    def get_required_attrs(self) -> Iterator[str]:
        """ Get the required attributes for this key """
