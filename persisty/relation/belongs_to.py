from dataclasses import dataclass
from typing import Optional, get_type_hints, Any

from marshy.factory.optional_marshaller_factory import get_optional_type

from persisty.entity.entity_context import get_named_entity_type
from persisty.relation.relation_abc import RelationABC
from persisty.util import to_snake_case, to_camel_case, UNDEFINED


@dataclass
class BelongsTo(RelationABC):
    name: Optional[str] = None  # Allows None so __set_name__ can exist
    storage_name: Optional[str] = None
    id_field_name: Optional[str] = None

    def __set_name__(self, owner, name):
        self.name = name
        if not self.storage_name:
            self.storage_name = self.name
        if self.id_field_name is None:
            self.id_field_name = f"{to_snake_case(self.name)}_id"
        annotations = get_type_hints(owner)
        assert annotations[self.id_field_name]

    def get_name(self) -> str:
        return self.name

    def to_property_descriptor(self):
        return BelongsToPropertyDescriptor(
            self.name, to_camel_case(self.storage_name), self.id_field_name, f"_{self.name}"
        )


@dataclass(frozen=True)
class BelongsToPropertyDescriptor:
    name: str
    entity_name: str
    id_field_name: str
    private_name: str

    def __get__(self, instance, owner):
        value = getattr(instance, self.private_name, UNDEFINED)
        if value is not UNDEFINED:
            return value
        key = getattr(instance, self.id_field_name)
        if not key:
            return None
        entity_type = get_named_entity_type(self.entity_name)
        value = entity_type.read(str(key), instance.__authorization__)
        if value:
            setattr(instance, self.private_name, value)
        return value

    def after_set_attr(self, instance, key: str, old_value: Any, new_value: Any):
        if key == self.id_field_name:
            # this indicates the intent is to change from belonging from one entity to another
            setattr(instance, self.private_name, UNDEFINED)
