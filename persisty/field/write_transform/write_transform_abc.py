from abc import abstractmethod, ABC
from typing import Union

from marshy import ExternalType

from persisty.field.write_transform.write_transform_mode import (
    WriteTransformMode,
)
from persisty.util.undefined import Undefined

T = Union[Undefined, ExternalType]


class WriteTransformABC(ABC):
    @abstractmethod
    def get_create_mode(self) -> WriteTransformMode:
        """Get the mode for this generator"""

    @abstractmethod
    def get_update_mode(self) -> WriteTransformMode:
        """Get the mode for this generator"""

    @abstractmethod
    def transform(self, specified_value: T, is_update: bool = False) -> T:
        """Transform this"""
