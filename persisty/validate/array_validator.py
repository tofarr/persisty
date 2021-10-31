from dataclasses import dataclass
from typing import Optional, List, Iterator

from persisty.validate.validation_error import ValidationError
from persisty.validate.validator_abc import ValidatorABC, T


@dataclass(frozen=True)
class ArrayValidator(ValidatorABC[List[T]]):
    items_validator: Optional[ValidatorABC[T]] = None
    min_items: int = 0
    max_items: Optional[int] = None
    uniqueness: bool = False

    def get_validation_errors(self,
                              items: List[T],
                              current_path: Optional[List[str]] = None
                              ) -> Iterator[ValidationError]:
        if current_path is None:
            current_path = []
        if self.items_validator is not None:
            for index, item in enumerate(items):
                current_path.append(str(index))
                yield from self.items_validator.get_validation_errors(item, current_path)
                current_path.pop()
        if self.min_items is not None and len(items) < self.min_items:
            yield ValidationError(current_path, 'min_length', items)
        if self.max_items is not None and len(items) > self.max_items:
            yield ValidationError(current_path, 'max_length', items)
