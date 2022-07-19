from abc import ABC, abstractmethod
from typing import Optional

from marshy.types import ExternalItemType


class KeyConfigABC(ABC):
    """ General object for sorting items """

    @abstractmethod
    def get_key(self, item: ExternalItemType) -> str:
        """ Get the key from the stored given. """

    @abstractmethod
    def set_key(self, key: Optional[str], item: ExternalItemType):
        """ Set the key for the stored given """

    @abstractmethod
    def is_required_field(self, field_name: str) -> bool:
        """ Determine whether the field given is required to be non null by this key config """
