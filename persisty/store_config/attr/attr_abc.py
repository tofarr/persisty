from typing import Generic

from schemey.schema_abc import SchemaABC

T = TypeVar('T')


class AttrABC(Generic[T]):
    name: str
    schema: SchemaABC[T]
    attr_access_control: AttrAccessControl
