from dataclasses import dataclass, MISSING, Field, is_dataclass, fields
from typing import Type, Optional, Iterable

from schemey.schema_abc import SchemaABC
from schemey.schema_context import SchemaContext, get_default_schema_context

from persisty.attr.attr_abc import AttrABC, A, B
from persisty.attr.attr_access_control import REQUIRED, OPTIONAL
from persisty.deferred.deferred_resolution_set import DeferredResolutionSet
from persisty.obj_graph.selections import Selections
from persisty.attr.attr_access_control_abc import AttrAccessControlABC

LOCAL_VALUES = '__local_values__'
REMOTE_VALUES = '__remote_values__'


@dataclass
class Attr(AttrABC[A, B]):
    name: Optional[str] = None
    type: Optional[Type[B]] = None
    schema: Optional[SchemaABC[B]] = None
    attr_access_control: Optional[AttrAccessControlABC] = None

    def __set_name__(self, owner, name):
        self.name = name
        if self.type is None:
            self.type = owner.__annotations__.get(self.name)
            if not self.type:
                raise ValueError(f'missing_annotation:{owner}:{self.name}')
        if self.schema is None:
            self.schema = get_default_schema_context().get_schema(self.type)
        if self.attr_access_control is None:
            self.access_control = REQUIRED if self.schema.default_value is MISSING else OPTIONAL

    def __get__(self, owner_instance: A, owner_type: Type[A]) -> B:
        local_values = getattr(owner_instance, LOCAL_VALUES, None)
        if local_values:
            value = getattr(local_values, self.name, MISSING)
        else:
            value = getattr(owner_instance, f'_{self.name}', MISSING)
        return value

    def __set__(self, owner_instance: A, value: B):
        local_values = getattr(owner_instance, LOCAL_VALUES, None)
        if local_values:
            setattr(local_values, self.name, value)
        else:
            setattr(owner_instance, f'_{self.name}', value)

    def unresolve(self, owner_instance: A):
        pass

    def is_resolved(self, owner_instance: A) -> bool:
        return True

    def is_save_required(self, owner_instance: A) -> bool:
        if not hasattr(owner_instance, LOCAL_VALUES) or not hasattr(owner_instance, REMOTE_VALUES):
            return False
        remote_values = owner_instance.__remote_values__
        local_values = owner_instance.__local_values__
        is_save_required = local_values.get(self.name) != remote_values.get(self.name)
        return is_save_required

    def resolve(self,
                owner_instance: A,
                sub_selections: Optional[Selections] = None,
                deferred_resolutions: Optional[DeferredResolutionSet] = None):
        pass  # nothing needs to be resolved


def attrs_from_class(cls) -> Iterable[AttrABC]:
    if hasattr(cls, '__attrs__'):
        return cls.__attrs__
    elif is_dataclass(cls):
        attrs = tuple(attr_from_field(f) for f in fields(cls))
        return attrs
    else:
        attrs = tuple(a for a in (c.__dict__.values() for c in cls.mro()) if isinstance(a, AttrABC))
        return attrs


def attr_from_field(field: Field, schema_context: SchemaContext = None, access_control: AttrAccessControlABC = None):
    schema = field.metadata.get('schema')
    if schema is not None and not isinstance(schema, SchemaABC):
        raise ValueError(f'not_an_instance:SchemaABC:{schema}')
    if not schema:
        if schema_context is None:
            schema_context = get_default_schema_context()
        default_value = None if field.default is MISSING else field.default

        schema = schema_context.get_schema(field.type, default_value)
    if access_control is None:
        if field.default is MISSING and field.default_factory is MISSING:
            access_control = REQUIRED
        else:
            access_control = OPTIONAL
    attr = Attr(field.name, field.type, schema, access_control)
    return attr
