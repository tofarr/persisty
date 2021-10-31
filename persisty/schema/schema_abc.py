from abc import abstractmethod, ABC
from typing import TypeVar, Generic, Iterator, Optional, List

from persisty.schema.schema_error import SchemaError

T = TypeVar('T')


class SchemaABC(ABC, Generic[T]):
    """ Validator for a particular type of object. By convention these are marshalled to json schema specs. """

    @abstractmethod
    def get_schema_errors(self, item: T, current_path: Optional[List[str]] = None) -> Iterator[SchemaError]:
        """ Get the validation errors for the item given. """
