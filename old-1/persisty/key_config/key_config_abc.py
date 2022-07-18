from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from persisty.attr.attr_mode import AttrMode

T = TypeVar('T')


class KeyConfigABC(ABC, Generic[T]):

    @property
    @abstractmethod
    def key_generation(self) -> AttrMode:
        """ Get the type of key generation """

    @abstractmethod
    def get_key(self, item: T) -> str:
        """ Get the key for the stored given """

    @abstractmethod
    def set_key(self, item: T, key: str):
        """ Set the key for the stored given """
