from dataclasses import dataclass
from typing import TypeVar, Generic, Iterator, Optional, List

from persisty.validate.validation_error import ValidationError
from persisty.validate.validator_abc import ValidatorABC, T

B = TypeVar('B')


@dataclass(frozen=True)
class PropertyValidator(Generic[T, B], ValidatorABC[T]):
    name: str
    validator: ValidatorABC[B]

    def get_validation_errors(self, item: T, current_path: Optional[List[str]] = None) -> Iterator[ValidationError]:
        attr = getattr(item, self.name, None)
        current_path.append(self.name)
        yield from self.validator.get_validation_errors(attr)
        current_path.pop()
