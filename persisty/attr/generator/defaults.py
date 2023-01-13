from datetime import datetime
from typing import Type, Optional, Tuple
from uuid import UUID

from persisty.attr.generator.attr_value_generator_abc import AttrValueGeneratorABC
from persisty.attr.generator.int_sequence_generator import IntSequenceGenerator
from persisty.attr.generator.str_sequence_generator import StrSequenceGenerator
from persisty.attr.generator.timestamp_generator import TimestampGenerator
from persisty.attr.generator.uuid_generator import UuidGenerator


def get_default_generator_for_create(
    name: str, type_: Type
) -> Tuple[bool, Optional[AttrValueGeneratorABC]]:
    if name == "id":
        if type_ == UUID:
            return True, UuidGenerator()
        elif type_ == str:
            return False, StrSequenceGenerator()
        elif type_ == int:
            return False, IntSequenceGenerator()
    elif type_ == datetime and name in ("created_at", "updated_at"):
        return False, TimestampGenerator()
    return True, None


def get_default_generator_for_update(
    name: str, type_: Type
) -> Tuple[bool, Optional[AttrValueGeneratorABC]]:
    if type_ == datetime and name == "updated_at":
        return False, TimestampGenerator()
    return True, None
