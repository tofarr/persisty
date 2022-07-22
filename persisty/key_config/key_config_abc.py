from abc import ABC, abstractmethod
from typing import Optional, Any

from marshy.types import ExternalItemType


class KeyConfigABC(ABC):
    """General object for sorting items"""

    @abstractmethod
    def to_key_str(self, item: ExternalItemType) -> str:
        """Get the key from the stored given."""

    @abstractmethod
    def from_key_str(self, key: Optional[str], output: Optional[Any] = None) -> Any:
        """Set the key for the stored given"""

    @abstractmethod
    def is_required_field(self, field_name: str) -> bool:
        """Determine whether the field given is required to be non null by this key config"""
