from abc import abstractmethod, ABC
from typing import Union

from marshy import ExternalType

from persisty.storage.field.write_transform.write_transform_mode import WriteTransformMode
from persisty.util.undefined import Undefined

T = Union[Undefined, ExternalType]


class WriteTransformABC(ABC):

    @abstractmethod
    @property
    def mode(self) -> WriteTransformMode:
        """ Get the mode for this generator """

    @abstractmethod
    def transform(self, specified_value: T, is_update: bool = False) -> T:
        """ Transform this  """
