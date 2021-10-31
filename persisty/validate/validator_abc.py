from abc import abstractmethod, ABC
from typing import TypeVar, Generic, Iterator, Optional, List

from persisty.validate.validation_error import ValidationError

T = TypeVar('T')


class ValidatorABC(ABC, Generic[T]):
    """ Validator for a particular type of object. By convention these are marshalled to json schema specs. """

    @abstractmethod
    def get_validation_errors(self, item: T, current_path: Optional[List[str]] = None) -> Iterator[ValidationError]:
        """ Get the validation errors for the item given. """
