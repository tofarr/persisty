from dataclasses import dataclass, MISSING
from typing import Type, Optional

from schemey.any_of_schema import optional_schema
from schemey.number_schema import NumberSchema
from schemey.schema_abc import SchemaABC

from persisty.attr.attr_abc import AttrABC, A, B
from persisty.attr.attr_access_control import AttrAccessControl, OPTIONAL
from persisty.deferred.deferred_resolution_set import DeferredResolutionSet
from persisty.entity.entity_abc import EntityABC
from persisty.entity.selections import Selections
from persisty.item_filter import AttrFilterOp, AttrFilter


@dataclass
class CountAttr(AttrABC[A, int]):
    name: str = None
    entity_type: Type[EntityABC] = None
    foreign_key_attr: str = None
    attr_access_control: AttrAccessControl = OPTIONAL

    @property
    def type(self):
        return int

    def __set_name__(self, owner, name):
        self.name = name

    def __get__(self, owner_instance: A, owner_type: Type[A]) -> int:
        value = getattr(owner_instance, f'_{self.name}', MISSING)
        if value is MISSING:
            self.resolve(owner_instance)
            value = getattr(owner_instance, f'_{self.name}')
        return value

    def __set__(self, owner_instance: A, value: int):
        setattr(owner_instance, f'_{self.name}', value)

    def unresolve(self, owner_instance: A):
        setattr(owner_instance, f'_{self.name}', MISSING)

    def is_resolved(self, owner_instance: A) -> bool:
        is_resolved = getattr(owner_instance, f'_{self.name}', MISSING) != MISSING
        return is_resolved

    def is_save_required(self, owner_instance: A) -> bool:
        return False

    def resolve(self,
                owner_instance: A,
                sub_selections: Optional[Selections] = None,
                deferred_resolutions: Optional[DeferredResolutionSet] = None):
        key = owner_instance.get_storage().meta.key_config.get_key(owner_instance)
        item_filter = AttrFilter(self.foreign_key_attr, AttrFilterOp.eq, key)
        entity_type = self.entity_type
        value: int = entity_type.count(item_filter)
        setattr(owner_instance, f'_{self.name}', value)

    @property
    def schema(self) -> SchemaABC[B]:
        schema = optional_schema(NumberSchema(int))
        return schema
