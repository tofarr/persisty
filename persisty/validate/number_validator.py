from dataclasses import dataclass
from typing import Optional, List, Iterator, Union, Type

from persisty.validate.validation_error import ValidationError
from persisty.validate.validator_abc import ValidatorABC

T = Union[int, float]


@dataclass(frozen=True)
class NumberValidator(ValidatorABC[T]):
    item_type: Type[T] = float
    minimum: Optional[T] = None
    exclusive_minimum: Optional[T] = None
    maximum: Optional[T] = None
    exclusive_maximum: Optional[T] = None

    def get_validation_errors(self, item: T, current_path: Optional[List[str]] = None) -> Iterator[ValidationError]:
        if not isinstance(item, self.item_type):
            yield ValidationError(current_path, 'type', item)
            return
        if self.minimum is not None and item < self.minimum:
            yield ValidationError(current_path, 'minimum', item)
        if self.exclusive_minimum is not None and item <= self.exclusive_minimum:
            yield ValidationError(current_path, 'exclusive_minimum', item)
        if self.maximum is not None and item > self.maximum:
            yield ValidationError(current_path, 'maximum', item)
        if self.exclusive_maximum is not None and item >= self.exclusive_maximum:
            yield ValidationError(current_path, 'exclusive_maximum', item)
