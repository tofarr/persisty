from dataclasses import dataclass
from typing import Optional, List, Iterator

from persisty.validate.validation_error import ValidationError
from persisty.validate.validator_abc import ValidatorABC


@dataclass(frozen=True)
class BooleanValidator(ValidatorABC[bool]):

    def get_validation_errors(self, item: bool, current_path: Optional[List[str]] = None) -> Iterator[ValidationError]:
        if not isinstance(item, bool):
            yield ValidationError(current_path, 'type', item)
