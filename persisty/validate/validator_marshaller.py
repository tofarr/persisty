from typing import Dict

from marshy.marshaller.marshaller_abc import MarshallerABC
from marshy.types import ExternalItemType

from persisty.util import filter_none
from persisty.validate.array_validator import ArrayValidator
from persisty.validate.boolean_validator import BooleanValidator
from persisty.validate.number_validator import NumberValidator
from persisty.validate.object_validator import ObjectValidator
from persisty.validate.optional_validator import OptionalValidator
from persisty.validate.property_validator import PropertyValidator
from persisty.validate.string_validator import StringValidator
from persisty.validate.validator_abc import ValidatorABC


class ValidatorMarshaller(MarshallerABC(ValidatorABC)):
    """ Marshaller which converts a validator to / from a json schema """

    def __init__(self):
        super().__init__(ValidatorABC)

    def load(self, item: ExternalItemType) -> ValidatorABC:
        item_type = item['type']
        optional = False
        if isinstance(item_type, list):
            optional = True
            item_type = next(i for i in item_type if i not in ['null', None])
        if item_type == 'boolean':
            return _input(optional, BooleanValidator())
        elif item_type in ['integer', 'number']:
            return _input(optional, NumberValidator(
                item_type=int if item_type == 'integer' else float,
                minimum=_load_attr(item, 'minimum', item_type),
                exclusive_minimum=_load_attr(item, 'exclusive_minimum', item_type),
                maximum=_load_attr(item, 'maximum', item_type),
                exclusive_maximum=_load_attr(item, 'exclusive_maximum', item_type),
            ))
        elif item_type == 'string':
            return _input(optional, StringValidator(
                min_length=_load_attr(item, 'minLength', item_type),
                max_length=_load_attr(item, 'maxLength', item_type),
                pattern=item.get('pattern'),
                format=item.get('format')
            ))
        elif item_type == 'array':
            return _input(optional, ArrayValidator(
                items_validator=self.load(item['items']) if 'items' in item else None,
                min_items=_load_attr(item, 'minItems', int),
                max_items=_load_attr(item, 'maxItems', int),
                uniqueness=_load_attr(item, 'uniqueness', bool),
            ))
        elif item_type == 'object':
            return _input(optional, ObjectValidator(
                property_validators=[PropertyValidator(k, self.load(v))
                                     for k, v in item.get('properties').items()]
            ))
        raise TypeError(f'unsupported_type:{item_type}')

    def dump(self, validator: ValidatorABC) -> ExternalItemType:
        optional = False
        if isinstance(validator, OptionalValidator):
            optional = True
            validator = validator.validator
        if isinstance(validator, BooleanValidator):
            return _output(optional, dict(type='boolean'))
        elif isinstance(validator, NumberValidator):
            return _output(optional, dict(
                type="integer" if validator.item_type == int else "number",
                minimum=validator.minimum,
                exclusiveMinimum=validator.exclusive_minimum,
                maximum=validator.maximum,
                exclusiveMaximum=validator.exclusive_maximum
            ))
        elif isinstance(validator, StringValidator):
            return _output(optional, dict(
                type='string',
                minLength=validator.min_length,
                maxLength=validator.max_length,
                pattern=validator.pattern,
                format=validator.format
            ))
        elif isinstance(validator, ArrayValidator):
            return _output(optional, dict(
                item=self.dump(validator.items_validator) if validator.items_validator else None,
                minItems=validator.min_items,
                maxItems=validator.max_items,
                uniqueness=validator.uniqueness
            ))
        elif isinstance(validator, ObjectValidator):
            return _output(optional, dict(
                properties={k: self.dump(v) for k, v in (validator.property_validators or {}).items()}
            ))
        raise TypeError(f'unsupported_type:{validator}')


def _load_attr(item: ExternalItemType, key: str, item_type):
    value = item.get(key)
    return None if item is None else item_type(value)


def _input(optional: bool, validator: ValidatorABC):
    if optional:
        validator = OptionalValidator(validator)
    return validator


def _output(optional: bool, item: Dict):
    if optional:
        item['type'] = [item['type'], None]
    return filter_none(item)

