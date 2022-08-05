from dataclasses import dataclass
from typing import Optional, get_type_hints

from persisty.entity.entity_context import get_named_entity_type
from persisty.errors import PersistyError
from persisty.field.field_filter import FieldFilter, FieldFilterOp
from persisty.link.link_abc import LinkABC
from persisty.util import to_camel_case, to_snake_case


@dataclass
class HasMany(LinkABC):
    name: Optional[str] = None  # Allows None so __set_name__ can exist
    storage_name: Optional[str] = None
    id_field_name: Optional[str] = None

    def __set_name__(self, owner, name):
        self.name = name
        if not self.storage_name:
            if name.endswith("_result_set"):
                self.storage_name = name[:-11]
            else:
                raise PersistyError(f"Please specify storage name for: {name}")
        if self.id_field_name is None:
            self.id_field_name = f"{to_snake_case(owner.__name__)}_id"

    def get_name(self) -> str:
        return self.name

    def to_property_descriptor(self):
        return HasManyPropertyDescriptor(
            self.name, to_camel_case(self.storage_name), self.id_field_name
        )


@dataclass(frozen=True)
class HasManyPropertyDescriptor:
    name: str
    entity_name: str
    id_field_name: str

    def __get__(self, instance, owner):
        key = instance.__persisty_storage_meta__.key_config.to_key_str(instance)
        if not key:
            return None
        entity_type = get_named_entity_type(self.entity_name)
        result_set = entity_type.search(
            FieldFilter(self.id_field_name, FieldFilterOp.eq, key)
        )
        return result_set
