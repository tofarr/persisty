from abc import ABC, abstractmethod
from typing import Optional, TypeVar, Generic

T = TypeVar('T')


class ObjKeyConfigABC(ABC, Generic[T]):
    """ General object for sorting items """

    @abstractmethod
    def get_key(self, item: T) -> str:
        """ Get the key from the stored given. """

    @abstractmethod
    def set_key(self, key: Optional[str], item: T):
        """ Set the key for the stored given """
