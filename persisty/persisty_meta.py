from dataclasses import dataclass
from typing import Optional

from schemey.schema_abc import SchemaABC

from persisty.capabilities import Capabilities


@dataclass(frozen=True)
class PersistyMeta:
    name: str
    capabilities: Capabilities
    schema_for_create: Optional[SchemaABC] = None
    schema_for_update: Optional[SchemaABC] = None
    schema_for_read: Optional[SchemaABC] = None
    schema_for_search: Optional[SchemaABC] = None
