from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from persisty2.key_config.key_generation import KeyGeneration

T = TypeVar('T')


class KeyConfigABC(ABC, Generic[T]):

    @abstractmethod
    @property
    def key_generation(self) -> KeyGeneration:
        """ Get the type of key generation """

    @abstractmethod
    def get_key(self, item: T) -> str:
        """ Get the key for the item given """

    @abstractmethod
    def set_key(self, item: T, key: str):
        """ Set the key for the item given """
