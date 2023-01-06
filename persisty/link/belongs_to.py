from dataclasses import dataclass
from typing import Optional, get_type_hints, Any

from marshy.factory.optional_marshaller_factory import get_optional_type
from marshy.types import ExternalItemType

from persisty.link.link_abc import LinkABC
from persisty.link.on_delete import OnDelete
from persisty.util import to_snake_case, to_camel_case, UNDEFINED


@dataclass
class BelongsTo(LinkABC):
    name: Optional[str] = None  # Allows None so __set_name__ can exist
    storage_name: Optional[str] = None
    id_field_name: Optional[str] = None
    optional: Optional[bool] = None
    on_delete: OnDelete = OnDelete.BLOCK

    def __set_name__(self, owner, name):
        self.name = name
        annotations = get_type_hints(owner, None, {owner.__name__: owner})
        type_ = annotations[name]
        if self.optional is None:
            optional_type = get_optional_type(type_)
            self.optional = bool(optional_type)
            if optional_type:
                type_ = optional_type
        if not self.storage_name:
            self.storage_name = to_snake_case(type_.__name__)
        if self.id_field_name is None:
            self.id_field_name = f"{self.name}_id"
        assert annotations[self.id_field_name]

    def get_name(self) -> str:
        return self.name

    def to_property_descriptor(self):
        return BelongsToPropertyDescriptor(
            self.name,
            to_camel_case(self.storage_name),
            self.id_field_name,
            f"_{self.name}",
        )

    def update_json_schema(self, json_schema: ExternalItemType):
        id_field_schema = json_schema.get("properties").get(self.id_field_name)
        id_field_schema["persistyBelongsTo"] = self.storage_name


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
