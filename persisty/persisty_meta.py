from dataclasses import dataclass
from typing import Generic, TypeVar

from schemey.schema_abc import SchemaABC

from persisty.capabilities import Capabilities

T = TypeVar('T')


@dataclass(frozen=True)
class PersistyMeta(Generic[T]):
    name: str
    capabilities: Capabilities
    schema_for_create: SchemaABC[T] = None
    schema_for_update: SchemaABC[T] = None
    schema_for_read: SchemaABC[T] = None
    schema_for_search: SchemaABC[T] = None
