from dataclasses import dataclass
from datetime import datetime, time
import re
from typing import Optional, List, Iterator

import validators

from persisty.validate.string_format import StringFormat
from persisty.validate.validation_error import ValidationError
from persisty.validate.validator_abc import ValidatorABC


@dataclass(frozen=True)
class StringValidator(ValidatorABC[str]):

    min_length: Optional[int] = None
    max_length: Optional[int] = None
    pattern: Optional[str] = None
    format: Optional[StringFormat] = None
    _compiled_pattern = None

    def __post_init__(self):
        object.__setattr__(self, '_compiled_pattern', None if self.pattern is None else re.compile(self.pattern))

    def get_validation_errors(self, item: str, current_path: Optional[List[str]] = None) -> Iterator[ValidationError]:
        if not isinstance(item, str):
            yield ValidationError(current_path, 'type', item)
            return
        if self.min_length is not None and len(item) < self.min_length:
            yield ValidationError(current_path, 'min_length', item)
        if self.max_length is not None and len(item) > self.max_length:
            yield ValidationError(current_path, 'max_length', item)
        if self._compiled_pattern is not None and not self._compiled_pattern.match(item):
            yield ValidationError(current_path, 'pattern', item)
        if self.format == StringFormat.DATE:
            if datetime.fromisoformat(item).isoformat()[:10] != item:
                yield ValidationError(current_path, 'format:email', item)
        elif self.format == StringFormat.DATE_TIME:
            if datetime.fromisoformat(item).isoformat() != item:
                yield ValidationError(current_path, 'format:email', item)
        elif self.format == StringFormat.EMAIL:
            if validators.email(item) is not True:
                yield ValidationError(current_path, 'format:email', item)
        elif self.format == StringFormat.HOSTNAME:
            if validators.domain(item) is not True:
                yield ValidationError(current_path, 'format:hostname', item)
        elif self.format == StringFormat.IPV4:
            if validators.ipv4(item) is not True:
                yield ValidationError(current_path, 'format:ipv4', item)
        elif self.format == StringFormat.IPV6:
            if validators.ipv6(item) is not True:
                yield ValidationError(current_path, 'format:ipv6', item)
        elif self.format == StringFormat.TIME:
            if time.fromisoformat(item).isoformat() != item:
                yield ValidationError(current_path, 'format:time', item)
        elif self.format == StringFormat.URI:
            if validators.url(item) is not True:
                yield ValidationError(current_path, 'format:uri', item)
        elif self.format == StringFormat.UUID:
            if validators.uuid(item) is not True:
                yield ValidationError(current_path, 'format:uuid', item)
