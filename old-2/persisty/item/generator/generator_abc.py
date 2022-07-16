from abc import abstractmethod, ABC
from typing import Generic, TypeVar, Union

from persisty.item.generator.generator_mode import GeneratorMode
from persisty.util.undefined import Undefined

T = TypeVar('T')


class GeneratorABC(ABC, Generic[T]):

    @abstractmethod
    @property
    def generator_mode(self) -> GeneratorMode:
        """ Get the write_transform mode """

    @abstractmethod
    def generate_value(self, specified_value: Union[Undefined, T], is_update: bool = False) -> Union[Undefined, T]:
        """ Generate a new value for this item """
