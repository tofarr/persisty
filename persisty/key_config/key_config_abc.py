from abc import ABC, abstractmethod
from typing import Generic, TypeVar, Optional

T = TypeVar('T')


class KeyConfigABC(ABC, Generic[T]):
    """ General object for sorting items """

    @abstractmethod
    def get_key(self, item: T) -> str:
        """ Get the key from the item given. """

    @abstractmethod
    def generate_key(self) -> str:
        """ Not all implementations support this... """

    @abstractmethod
    def set_key(self, key: str, item: T):
        """ Set the key for the item given """
