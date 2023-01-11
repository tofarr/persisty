from datetime import datetime
from typing import Type, Optional, Tuple
from uuid import UUID

from aaaa.attr.generator.attr_value_generator_abc import AttrValueGeneratorABC
from aaaa.attr.generator.uuid_generator import UuidGenerator


def get_default_generator_for_create(name: str, type_: Type) -> Tuple[bool, Optional[AttrValueGeneratorABC]]:
    if type_ == UUID and name == 'id':
        return False, UuidGenerator()
    elif type_ == datetime and name in ('created_at', 'updated_at'):
        return False, UuidGenerator()
    return True, None


def get_default_generator_for_update(name: str, type_: Type) -> Tuple[bool, Optional[AttrValueGeneratorABC]]:
    if type_ == datetime and name == 'updated_at':
        return False, UuidGenerator()
    return True, None
