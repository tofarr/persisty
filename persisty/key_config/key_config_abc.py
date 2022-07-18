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
