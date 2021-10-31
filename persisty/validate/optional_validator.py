from dataclasses import dataclass
from typing import TypeVar, Iterator, Optional, List

from persisty.validate.validation_error import ValidationError
from persisty.validate.validator_abc import ValidatorABC

T = TypeVar('T')


@dataclass(frozen=True)
class OptionalValidator(ValidatorABC[T]):
    validator: ValidatorABC[T]

    def get_validation_errors(self,
                              item: Optional[T],
                              current_path: Optional[List[str]] = None
                              ) -> Iterator[ValidationError]:
        if item is not None:
            yield from self.validator.get_validation_errors(item, current_path)
