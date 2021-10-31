from dataclasses import dataclass, field, fields, is_dataclass
from typing import Iterable, Union, Sized, Optional, List, Iterator, Type

from marshy.factory.optional_marshaller_factory import get_optional_type

from persisty.validate.boolean_validator import BooleanValidator
from persisty.validate.number_validator import NumberValidator
from persisty.validate.optional_validator import OptionalValidator
from persisty.validate.property_validator import PropertyValidator
from persisty.validate.string_validator import StringValidator
from persisty.validate.validation_error import ValidationError
from persisty.validate.validator_abc import ValidatorABC, T


@dataclass(frozen=True)
class ObjectValidator(ValidatorABC[T]):
    property_validators: Union[Iterable[PropertyValidator], Sized] = field(default_factory=list)

    def get_validation_errors(self, item: T, current_path: Optional[List[str]] = None) -> Iterator[ValidationError]:
        for property_validator in (self.property_validators or []):
            yield from property_validator.get_validation_errors(item, current_path)


def validator_from_dataclass(cls: Type[T]) -> ObjectValidator[T]:
    if hasattr(cls, '__validator__'):
        return cls.__validator__
    property_validators = []
    for f in fields(cls):
        validator = f.metadata['validator']
        if isinstance(validator, ValidatorABC):
            property_validators.append(PropertyValidator(f.name, validator))
            continue
        field_type = f.type
        optional = False
        validator = None
        optional_type = get_optional_type(f.type)
        if optional_type:
            field_type = optional_type
            optional = True
        if field_type == str:
            validator = StringValidator()
        elif field_type in [int, float]:
            validator = NumberValidator(field_type)
        elif field_type == bool:
            validator = BooleanValidator()
        elif is_dataclass(field_type):
            validator = validator_from_dataclass(field_type)
        if optional and validator:
            validator = OptionalValidator(validator)
        if validator:
            property_validators.append(PropertyValidator(f.name, validator))
    return ObjectValidator(property_validators)
