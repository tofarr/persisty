from dataclasses import dataclass, Field, field, MISSING
from typing import Type, Optional

from schemey.schema_abc import SchemaABC

from persisty2.attr.attr_abc import AttrABC, A, B
from persisty2.attr.attr_access_control import AttrAccessControl
from persisty2.deferred.deferred_resolution_set import DeferredResolutionSet
from persisty2.selections import Selections


@dataclass
class Attr(AttrABC[A, B]):
    name: str
    type: Type[B]
    schema: SchemaABC[B]
    attr_access_control: AttrAccessControl

    def __set_name__(self, owner, name):
        self.name = name
        if self.type is None:
            self.type = owner.__annotations__.get(self.name)
            if not self.type:
                raise ValueError(f'missing_annotation:{owner}:{self.name}')

    def __get__(self, owner_instance: A, owner_type: Type[A]) -> B:
        local_values = owner_instance.local_values
        value = getattr(local_values, self.name, MISSING)
        return value

    def __set__(self, owner_instance: A, value: B):
        local_values = owner_instance.local_values
        setattr(local_values, self.name, value)

    def unresolve(self, owner_instance: A):
        pass

    def is_resolved(self, owner_instance: A) -> bool:
        return True

    def is_save_required(self, owner_instance: A) -> bool:
        remote_values = owner_instance.remote_values
        local_values = owner_instance.local_values
        is_save_required = local_values.get(self.name) != remote_values.get(self.name)
        return is_save_required

    def resolve(self,
                owner_instance: A,
                sub_selections: Optional[Selections] = None,
                deferred_resolutions: Optional[DeferredResolutionSet] = None):
        pass  # nothing needs to be resolved
